// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//  ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    internal class PartitionProcessorFactory : IPartitionProcessorFactory
    {
        private readonly IChangeFeedDocumentClient documentClient;
        private readonly ChangeFeedHostOptions changeFeedHostOptions;
        private readonly ChangeFeedOptions changeFeedOptions;
        private readonly ILeaseManager leaseManager;
        private readonly string collectionSelfLink;

        public PartitionProcessorFactory(IChangeFeedDocumentClient documentClient, ChangeFeedHostOptions changeFeedHostOptions, ChangeFeedOptions changeFeedOptions, ILeaseManager leaseManager, string collectionSelfLink)
        {
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            if (changeFeedHostOptions == null) throw new ArgumentNullException(nameof(changeFeedHostOptions));
            if (changeFeedOptions == null) throw new ArgumentNullException(nameof(changeFeedHostOptions));
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            if (collectionSelfLink == null) throw new ArgumentNullException(nameof(collectionSelfLink));

            this.documentClient = documentClient;
            this.changeFeedHostOptions = changeFeedHostOptions;
            this.leaseManager = leaseManager;
            this.collectionSelfLink = collectionSelfLink;
            this.changeFeedOptions = changeFeedOptions;
        }

        public IPartitionProcessor Create(ILease lease, IChangeFeedObserver observer)
        {
            if (observer == null) throw new ArgumentNullException(nameof(observer));
            if (lease == null) throw new ArgumentNullException(nameof(lease));

            var settings = new ProcessorSettings
            {
                CollectionSelfLink = this.collectionSelfLink,
                RequestContinuation = lease.ContinuationToken,
                PartitionKeyRangeId = lease.PartitionId,
                FeedPollDelay = this.changeFeedHostOptions.FeedPollDelay,
                MaxItemCount = this.changeFeedOptions.MaxItemCount,
                StartFromBeginning = this.changeFeedOptions.StartFromBeginning,
                StartTime = this.changeFeedOptions.StartTime,
                SessionToken = this.changeFeedOptions.SessionToken,
            };

            var checkpointer = new PartitionCheckpointer(this.leaseManager, lease);
            return new PartitionProcessor(observer, this.documentClient, settings, checkpointer);
        }
    }
}
