using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DocumentDB.ChangeFeedProcessor.Exceptions;
using DocumentDB.ChangeFeedProcessor.Logging;
using DocumentDB.ChangeFeedProcessor.Utils;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    internal class PartitionController : IPartitionController
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly string hostName;

        private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> currentlyOwnedPartitions = new ConcurrentDictionary<string, TaskCompletionSource<bool>>();

        private readonly ILeaseManager leaseManager;
        private readonly IPartitionSupervisorFactory partitionSupervisorFactory;
        private readonly IPartitionSynchronizer synchronizer;
        private readonly CancellationTokenSource shutdownCts = new CancellationTokenSource();

        public PartitionController(string hostName, ILeaseManager leaseManager, IPartitionSupervisorFactory partitionSupervisorFactory, IPartitionSynchronizer synchronizer)
        {
            this.hostName = hostName;
            this.leaseManager = leaseManager;
            this.partitionSupervisorFactory = partitionSupervisorFactory;
            this.synchronizer = synchronizer;
        }

        public async Task InitializeAsync()
        {
            await LoadLeasesAsync().ConfigureAwait(false);
        }

        private async Task LoadLeasesAsync()
        {
            logger.Debug("Starting renew leases assigned to this host on initialize.");
            var addLeaseTasks = new List<Task>();
            foreach (ILease lease in await leaseManager.ListLeasesAsync().ConfigureAwait(false))
            {
                if (string.Compare(lease.Owner, hostName, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    logger.InfoFormat("Acquired lease for PartitionId '{0}' on startup.", lease.PartitionId);
                    addLeaseTasks.Add(AddLeaseAsync(lease));
                }
            }
            await Task.WhenAll(addLeaseTasks.ToArray()).ConfigureAwait(false);
        }

        public async Task AddLeaseAsync(ILease lease)
        {
            var tcs = new TaskCompletionSource<bool>();
            if (!currentlyOwnedPartitions.TryAdd(lease.PartitionId, tcs))
            {
                return;
            }

            try
            {
                await leaseManager.AcquireAsync(lease, hostName).ConfigureAwait(false);
                logger.InfoFormat("partition {0}: acquired", lease.PartitionId);
            }
            catch (Exception)
            {
                await RemoveLeaseAsync(lease).ConfigureAwait(false);
                throw;
            }

            IPartitionSupervisor supervisor = partitionSupervisorFactory.Create(lease);
            ProcessPartition(supervisor, lease).LogException();
        }

        private async Task RemoveLeaseAsync(ILease lease)
        {
            TaskCompletionSource<bool> worker;
            if (!currentlyOwnedPartitions.TryRemove(lease.PartitionId, out worker))
            {
                return;
            }

            logger.InfoFormat("partition {0}: released", lease.PartitionId);

            try
            {
                await leaseManager.ReleaseAsync(lease).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                logger.WarnException("partition {0}: failed to remove lease", e, lease.PartitionId);
            }
            finally
            {
                worker.SetResult(false);
            }
        }

        public async Task ShutdownAsync()
        {
            shutdownCts.Cancel();
            IEnumerable<Task> leases = currentlyOwnedPartitions.Select(pair => pair.Value.Task).ToList();
            await Task.WhenAll(leases).ConfigureAwait(false);
        }

        private async Task ProcessPartition(IPartitionSupervisor partitionSupervisor, ILease lease)
        {
            try
            {
                await partitionSupervisor.RunAsync(shutdownCts.Token).ConfigureAwait(false);
            }
            catch (PartitionSplitException)
            {
                await HandleSplitAsync(lease).ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                logger.DebugFormat("partition {0}: processing canceled", lease.PartitionId);
            }
            catch (Exception e)
            {
                logger.WarnException("partition {0}: processing failed", e, lease.PartitionId);
            }

            await RemoveLeaseAsync(lease).ConfigureAwait(false);
        }

        private async Task HandleSplitAsync(ILease lease)
        {
            try
            {
                IEnumerable<ILease> addedLeases = await synchronizer.SplitPartitionAsync(lease).ConfigureAwait(false);
                Task[] addLeaseTasks = addedLeases.Select(AddLeaseAsync).ToArray();
                await leaseManager.DeleteAsync(lease).ConfigureAwait(false);
                await Task.WhenAll(addLeaseTasks).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                logger.WarnException("partition {0}: failed to split", e, lease.PartitionId);
            }
        }
    }
}