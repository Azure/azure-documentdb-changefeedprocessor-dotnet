﻿// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//  ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class PartitionProcessorFactory : IPartitionProcessorFactory
    {
        private readonly IChangeFeedDocumentClient documentClient;
        private readonly ChangeFeedProcessorOptions changeFeedProcessorOptions;
        private readonly ILeaseCheckpointer leaseCheckpointer;
        private readonly string collectionSelfLink;
        private readonly IHealthMonitor healthMonitor;

        public PartitionProcessorFactory(
            IChangeFeedDocumentClient documentClient,
            ChangeFeedProcessorOptions changeFeedProcessorOptions,
            ILeaseCheckpointer leaseCheckpointer,
            string collectionSelfLink,
            IHealthMonitor healthMonitor)
        {
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            if (changeFeedProcessorOptions == null) throw new ArgumentNullException(nameof(changeFeedProcessorOptions));
            if (leaseCheckpointer == null) throw new ArgumentNullException(nameof(leaseCheckpointer));
            if (collectionSelfLink == null) throw new ArgumentNullException(nameof(collectionSelfLink));
            if (healthMonitor == null) throw new ArgumentNullException(nameof(healthMonitor));

            this.documentClient = documentClient;
            this.changeFeedProcessorOptions = changeFeedProcessorOptions;
            this.leaseCheckpointer = leaseCheckpointer;
            this.collectionSelfLink = collectionSelfLink;
            this.healthMonitor = healthMonitor;
        }

        public IPartitionProcessor Create(ILease lease, IChangeFeedObserver observer)
        {
            if (observer == null) throw new ArgumentNullException(nameof(observer));
            if (lease == null) throw new ArgumentNullException(nameof(lease));

            var settings = new ProcessorSettings
            {
                CollectionSelfLink = this.collectionSelfLink,
                StartContinuation = !string.IsNullOrEmpty(lease.ContinuationToken) ?
                    lease.ContinuationToken :
                    this.changeFeedProcessorOptions.StartContinuation,
                PartitionKeyRangeId = lease.PartitionId,
                FeedPollDelay = this.changeFeedProcessorOptions.FeedPollDelay,
                MaxItemCount = this.changeFeedProcessorOptions.MaxItemCount,
                StartFromBeginning = this.changeFeedProcessorOptions.StartFromBeginning,
                StartTime = this.changeFeedProcessorOptions.StartTime,
                SessionToken = this.changeFeedProcessorOptions.SessionToken,
                ChangeFeedTimeout = this.changeFeedProcessorOptions.ChangeFeedTimeout,
            };

            var checkpointer = new PartitionCheckpointer(this.leaseCheckpointer, lease);
            return new PartitionProcessor(observer, this.documentClient, settings, checkpointer, this.healthMonitor);
        }
    }
}
