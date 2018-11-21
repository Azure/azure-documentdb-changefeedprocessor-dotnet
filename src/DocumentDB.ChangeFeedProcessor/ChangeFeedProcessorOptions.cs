//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Options to control various aspects of partition distribution happening within <see cref="ChangeFeedProcessor"/> instance.
    /// </summary>
    public class ChangeFeedProcessorOptions
    {
        private const int DefaultQueryPartitionsMaxBatchSize = 100;
        private static readonly TimeSpan DefaultRenewInterval = TimeSpan.FromSeconds(17);
        private static readonly TimeSpan DefaultAcquireInterval = TimeSpan.FromSeconds(13);
        private static readonly TimeSpan DefaultExpirationInterval = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan DefaultFeedPollDelay = TimeSpan.FromSeconds(5);
        private DateTime? startTime;

        /// <summary>Initializes a new instance of the <see cref="ChangeFeedProcessorOptions" /> class.</summary>
        public ChangeFeedProcessorOptions()
        {
            this.LeaseRenewInterval = DefaultRenewInterval;
            this.LeaseAcquireInterval = DefaultAcquireInterval;
            this.LeaseExpirationInterval = DefaultExpirationInterval;
            this.FeedPollDelay = DefaultFeedPollDelay;
            this.QueryPartitionsMaxBatchSize = DefaultQueryPartitionsMaxBatchSize;
            this.CheckpointFrequency = new CheckpointFrequency();
        }

        /// <summary>
        /// Gets or sets renew interval for all leases for partitions currently held by <see cref="ChangeFeedProcessor"/> instance.
        /// </summary>
        public TimeSpan LeaseRenewInterval { get; set; }

        /// <summary>
        /// Gets or sets the interval to kick off a task to compute if partitions are distributed evenly among known host instances.
        /// </summary>
        public TimeSpan LeaseAcquireInterval { get; set; }

        /// <summary>
        /// Gets or sets the interval for which the lease is taken on a lease representing a partition. If the lease is not renewed within this
        /// interval, it will cause it to expire and ownership of the partition will move to another <see cref="ChangeFeedProcessor"/> instance.
        /// </summary>
        public TimeSpan LeaseExpirationInterval { get; set; }

        /// <summary>
        /// Gets or sets the delay in between polling a partition for new changes on the feed, after all current changes are drained.
        /// </summary>
        public TimeSpan FeedPollDelay { get; set; }

        /// <summary>
        /// Gets or sets the frequency how often to checkpoint leases.
        /// </summary>
        public CheckpointFrequency CheckpointFrequency { get; set; }

        /// <summary>
        /// Gets or sets a prefix to be used as part of the lease id. This can be used to support multiple instances of <see cref="ChangeFeedProcessor"/>
        /// instances pointing at the same feed while using the same auxiliary collection.
        /// </summary>
        public string LeasePrefix { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of items to be returned in the enumeration operation in the Azure Cosmos DB service.
        /// </summary>
        public int? MaxItemCount { get; set; }

        /// <summary>
        /// Gets or sets the start request continuation token to start looking for changes after.
        /// </summary>
        /// <remarks>
        /// This is only used when lease store is not initialized and is ignored if a lease for partition exists and has continuation token.
        /// If this is specified, both StartTime and StartFromBeginning are ignored.
        /// </remarks>
        /// <seealso cref="ChangeFeedOptions.RequestContinuation"/>
        public string StartContinuation { get; set; }

        /// <summary>
        /// Gets or sets the time (exclusive) to start looking for changes after.
        /// </summary>
        /// <remarks>
        /// This is only used when:
        /// (1) Lease store is not initialized and is ignored if a lease for partition exists and has continuation token.
        /// (2) StartContinuation is not specified.
        /// If this is specified, StartFromBeginning is ignored.
        /// </remarks>
        /// <seealso cref="ChangeFeedOptions.StartTime"/>
        public DateTime? StartTime
        {
            get
            {
                return this.startTime;
            }

            set
            {
                if (value.HasValue && value.Value.Kind == DateTimeKind.Unspecified)
                    throw new ArgumentException("StartTime cannot have DateTimeKind.Unspecified", nameof(value));
                this.startTime = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether change feed in the Azure Cosmos DB service should start from beginning (true) or from current (false).
        /// By default it's start from current (false).
        /// </summary>
        /// <remarks>
        /// This is only used when:
        /// (1) Lease store is not initialized and is ignored if a lease for partition exists and has continuation token.
        /// (2) StartContinuation is not specified.
        /// (3) StartTime is not specified.
        /// </remarks>
        /// <seealso cref="ChangeFeedOptions.StartFromBeginning"/>
        public bool StartFromBeginning { get; set; }

        /// <summary>
        /// Gets or sets the session token for use with session consistency in the Azure Cosmos DB service.
        /// </summary>
        public string SessionToken { get; set; }

        /// <summary>
        /// Gets or sets the minimum partition count for the host.
        /// This can be used to increase the number of partitions for the host and thus override equal distribution (which is the default) of leases between hosts.
        /// </summary>
        internal int MinPartitionCount { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of partitions the host can serve.
        /// This can be used property to limit the number of partitions for the host and thus override equal distribution (which is the default) of leases between hosts.
        /// Default is 0 (unlimited).
        /// </summary>
        internal int MaxPartitionCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether on start of the host all existing leases should be deleted and the host should start from scratch.
        /// </summary>
        internal bool DiscardExistingLeases { get; set; }

        /// <summary>
        /// Gets or sets the Batch size of query partitions API.
        /// </summary>
        internal int QueryPartitionsMaxBatchSize { get; set; }

        /// <summary>
        /// Gets maximum number of tasks to use for auxiliary calls.
        /// </summary>
        internal int DegreeOfParallelism => this.MaxPartitionCount > 0 ? this.MaxPartitionCount : 25;
    }
}