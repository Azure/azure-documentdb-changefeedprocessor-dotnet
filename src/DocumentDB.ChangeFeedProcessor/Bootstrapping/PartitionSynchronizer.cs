//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
    using Microsoft.Azure.Documents.Client;

    internal class PartitionSynchronizer : IPartitionSynchronizer
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly IChangeFeedDocumentClient documentClient;
        private readonly string collectionSelfLink;
        private readonly ILeaseManager leaseManager;
        private readonly int degreeOfParallelism;
        private readonly int maxBatchSize;

        public PartitionSynchronizer(IChangeFeedDocumentClient documentClient, string collectionSelfLink, ILeaseManager leaseManager, int degreeOfParallelism, int maxBatchSize)
        {
            this.documentClient = documentClient;
            this.collectionSelfLink = collectionSelfLink;
            this.leaseManager = leaseManager;
            this.degreeOfParallelism = degreeOfParallelism;
            this.maxBatchSize = maxBatchSize;
        }

        public async Task CreateMissingLeasesAsync()
        {
            List<PartitionKeyRange> ranges = await this.EnumPartitionKeyRangesAsync().ConfigureAwait(false);
            var partitionIds = new HashSet<string>(ranges.Select(range => range.Id));
            Logger.InfoFormat("Source collection: '{0}', {1} partition(s)", this.collectionSelfLink, partitionIds.Count);
            await this.CreateLeasesAsync(partitionIds).ConfigureAwait(false);
        }

        public async Task<IEnumerable<ILease>> SplitPartitionAsync(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            string partitionId = lease.PartitionId;
            string lastContinuationToken = lease.ContinuationToken;

            Logger.InfoFormat("Partition {0} is gone due to split", partitionId);
            List<PartitionKeyRange> ranges = await this.EnumPartitionKeyRangesAsync().ConfigureAwait(false);
            List<string> addedPartitionIds = ranges.Where(range => range.Parents.Contains(partitionId)).Select(range => range.Id).ToList();
            if (addedPartitionIds.Count < 2)
            {
                Logger.ErrorFormat("Partition {0} had split but we failed to find at least 2 child partitions", partitionId);
                throw new InvalidOperationException();
            }

            var newLeases = new ConcurrentQueue<ILease>();
            await addedPartitionIds.ForEachAsync(
                async addedRangeId =>
                {
                    ILease newLease = await this.leaseManager.CreateLeaseIfNotExistAsync(addedRangeId, lastContinuationToken).ConfigureAwait(false);
                    if (newLease != null)
                    {
                        newLeases.Enqueue(newLease);
                    }
                },
                this.degreeOfParallelism).ConfigureAwait(false);

            if (Logger.IsInfoEnabled())
            {
                Logger.InfoFormat("partition {0} split into {1}", partitionId, string.Join(", ", newLeases.Select(l => l.PartitionId)));
            }

            return newLeases;
        }

        private async Task<List<PartitionKeyRange>> EnumPartitionKeyRangesAsync()
        {
            string partitionKeyRangesPath = string.Format(CultureInfo.InvariantCulture, "{0}/pkranges", this.collectionSelfLink);

            IFeedResponse<PartitionKeyRange> response = null;
            var partitionKeyRanges = new List<PartitionKeyRange>();
            do
            {
                var feedOptions = new FeedOptions
                {
                    MaxItemCount = this.maxBatchSize,
                    RequestContinuation = response?.ResponseContinuation,
                };
                response = await this.documentClient.ReadPartitionKeyRangeFeedAsync(partitionKeyRangesPath, feedOptions).ConfigureAwait(false);
                IEnumerator<PartitionKeyRange> enumerator = response.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    partitionKeyRanges.Add(enumerator.Current);
                }
            }
            while (!string.IsNullOrEmpty(response.ResponseContinuation));

            return partitionKeyRanges;
        }

        /// <summary>
        /// Creates leases if they do not exist. This might happen on initial start or if some lease was unexpectedly lost.
        /// Leases are created without the continuation token. It means partitions will be read according to 'From Beginning' or
        /// 'From current time'.
        /// Same applies also to split partitions. We do not search for parent lease and take continuation token since this might end up
        /// of reprocessing all the events since the split.
        /// </summary>
        private async Task CreateLeasesAsync(HashSet<string> partitionIds)
        {
            // Get leases after getting ranges, to make sure that no other hosts checked in continuation for split partition after we got leases.
            IEnumerable<ILease> leases = await this.leaseManager.ListAllLeasesAsync().ConfigureAwait(false);
            var existingPartitionIds = new HashSet<string>(leases.Select(lease => lease.PartitionId));
            var addedPartitionIds = new HashSet<string>(partitionIds);
            addedPartitionIds.ExceptWith(existingPartitionIds);

            await addedPartitionIds.ForEachAsync(
                async addedRangeId => { await this.leaseManager.CreateLeaseIfNotExistAsync(addedRangeId, continuationToken: null).ConfigureAwait(false); },
                this.degreeOfParallelism).ConfigureAwait(false);
        }
    }
}