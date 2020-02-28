// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//  ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    internal class PartitionProcessorFactory : IPartitionProcessorFactory
    {
        private readonly IChangeFeedDocumentClient documentClient;
        private readonly ChangeFeedProcessorOptions changeFeedProcessorOptions;
        private readonly string collectionSelfLink;
        private readonly IHealthMonitor healthMonitor;

        public PartitionProcessorFactory(
            IChangeFeedDocumentClient documentClient,
            ChangeFeedProcessorOptions changeFeedProcessorOptions,
            string collectionSelfLink,
            IHealthMonitor healthMonitor)
        {
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            if (changeFeedProcessorOptions == null) throw new ArgumentNullException(nameof(changeFeedProcessorOptions));
            if (collectionSelfLink == null) throw new ArgumentNullException(nameof(collectionSelfLink));
            if (healthMonitor == null) throw new ArgumentNullException(nameof(healthMonitor));

            this.documentClient = documentClient;
            this.changeFeedProcessorOptions = changeFeedProcessorOptions;
            this.collectionSelfLink = collectionSelfLink;
            this.healthMonitor = healthMonitor;
        }

        public IPartitionProcessor Create(ILease lease, ILeaseCheckpointer leaseCheckpointer, IChangeFeedObserver observer)
        {
            if (observer == null) throw new ArgumentNullException(nameof(observer));
            if (lease == null) throw new ArgumentNullException(nameof(lease));
            if (leaseCheckpointer == null) throw new ArgumentNullException(nameof(leaseCheckpointer));

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
            };

            var checkpointer = new PartitionCheckpointer(leaseCheckpointer, lease);

            var changeFeedOptions = new ChangeFeedOptions
            {
                MaxItemCount = settings.MaxItemCount,
                PartitionKeyRangeId = settings.PartitionKeyRangeId,
                SessionToken = settings.SessionToken,
                StartFromBeginning = settings.StartFromBeginning,
                RequestContinuation = settings.StartContinuation,
                StartTime = settings.StartTime,
            };
            var changeFeedQuery = this.documentClient.CreateDocumentChangeFeedQuery(settings.CollectionSelfLink, changeFeedOptions);
            changeFeedQuery = new ChangeFeedQueryTimeoutDecorator(changeFeedQuery, this.healthMonitor, this.changeFeedProcessorOptions.ChangeFeedTimeout, lease);

            return new PartitionProcessor(observer, changeFeedQuery, changeFeedOptions, settings, checkpointer);
        }
    }
}
