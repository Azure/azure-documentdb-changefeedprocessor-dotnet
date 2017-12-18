using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal class EqualPartitionsBalancingStrategy : ILoadBalancingStrategy
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly string hostName;
        private readonly int minPartitionCount;
        private readonly int maxPartitionCount;
        private readonly TimeSpan leaseExpirationInterval;

        public EqualPartitionsBalancingStrategy(string hostName, int minPartitionCount, int maxPartitionCount, TimeSpan leaseExpirationInterval)
        {
            if (hostName == null) throw new ArgumentNullException(nameof(hostName));
            this.hostName = hostName;
            this.minPartitionCount = minPartitionCount;
            this.maxPartitionCount = maxPartitionCount;
            this.leaseExpirationInterval = leaseExpirationInterval;
        }

        public IEnumerable<ILease> CalculateLeasesToTake(IEnumerable<ILease> allLeases)
        {
            var workerToPartitionCount = new Dictionary<string, int>();
            var expiredLeases = new List<ILease>();
            var allPartitions = new Dictionary<string, ILease>();
            CategorizeLeases(allLeases, allPartitions, expiredLeases, workerToPartitionCount);

            int partitionCount = allPartitions.Count;
            int workerCount = workerToPartitionCount.Count;
            if (partitionCount <= 0)
                return Enumerable.Empty<ILease>();

            int target = CalculateTargetPartitionCount(partitionCount, workerCount);
            int myCount = workerToPartitionCount[hostName];
            int partitionsNeededForMe = target - myCount;

            logger.InfoFormat(
                "Host '{0}' {1} partitions, {2} hosts, {3} available leases, target = {4}, min = {5}, max = {6}, mine = {7}, will try to take {8} lease(s) for myself'.",
                hostName,
                partitionCount,
                workerCount,
                expiredLeases.Count,
                target,
                minPartitionCount,
                maxPartitionCount,
                myCount,
                Math.Max(partitionsNeededForMe, 0));

            if (partitionsNeededForMe <= 0)
                return Enumerable.Empty<ILease>();

            if (expiredLeases.Count > 0)
            {
                return expiredLeases.Take(partitionsNeededForMe);
            }
            ILease stolenLease = GetLeaseToSteal(workerToPartitionCount, target, partitionsNeededForMe, allPartitions);
            return stolenLease == null ? Enumerable.Empty<ILease>() : new[] { stolenLease };
        }

        private static ILease GetLeaseToSteal(Dictionary<string, int> workerToPartitionCount, int target, int partitionsNeededForMe, Dictionary<string, ILease> allPartitions)
        {
            KeyValuePair<string, int> workerToStealFrom = FindWorkerWithMostPartitions(workerToPartitionCount);
            if (workerToStealFrom.Value > target - (partitionsNeededForMe > 1 ? 1 : 0))
            {
                return allPartitions.Values.First(partition => string.Equals(partition.Owner, workerToStealFrom.Key, StringComparison.OrdinalIgnoreCase));
            }
            return null;
        }

        private static KeyValuePair<string, int> FindWorkerWithMostPartitions(Dictionary<string, int> workerToPartitionCount)
        {
            KeyValuePair<string, int> workerToStealFrom = default(KeyValuePair<string, int>);
            foreach (KeyValuePair<string, int> kvp in workerToPartitionCount)
            {
                if (workerToStealFrom.Value <= kvp.Value)
                {
                    workerToStealFrom = kvp;
                }
            }
            return workerToStealFrom;
        }

        private int CalculateTargetPartitionCount(int partitionCount, int workerCount)
        {
            var target = 1;
            if (partitionCount > workerCount)
            {
                target = (int)Math.Ceiling((double)partitionCount / workerCount);
            }
            if (maxPartitionCount > 0 && target > maxPartitionCount)
            {
                target = maxPartitionCount;
            }
            if (minPartitionCount > 0 && target < minPartitionCount)
            {
                target = minPartitionCount;
            }
            return target;
        }

        private void CategorizeLeases(IEnumerable<ILease> allLeases, Dictionary<string, ILease> allPartitions, List<ILease> expiredLeases, Dictionary<string, int> workerToPartitionCount)
        {
            foreach (ILease lease in allLeases)
            {
                Debug.Assert(lease.PartitionId != null, "TakeLeasesAsync: lease.PartitionId cannot be null.");

                allPartitions.Add(lease.PartitionId, lease);
                if (string.IsNullOrWhiteSpace(lease.Owner) || IsExpired(lease))
                {
                    logger.DebugFormat("Found unused or expired lease: {0}", lease);
                    expiredLeases.Add(lease);
                }
                else
                {
                    var count = 0;
                    string assignedTo = lease.Owner;
                    if (workerToPartitionCount.TryGetValue(assignedTo, out count))
                    {
                        workerToPartitionCount[assignedTo] = count + 1;
                    }
                    else
                    {
                        workerToPartitionCount.Add(assignedTo, 1);
                    }
                }
            }
            if (!workerToPartitionCount.ContainsKey(hostName))
            {
                workerToPartitionCount.Add(hostName, 0);
            }
        }

        private bool IsExpired(ILease lease)
        {
            return lease.Timestamp.ToUniversalTime() + leaseExpirationInterval < DateTime.UtcNow;
        }
    }
}