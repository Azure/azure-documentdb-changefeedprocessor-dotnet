//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using Client;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;

    internal class PartitionSupervisorFactory : IPartitionSupervisorFactory
    {
        private readonly FeedProcessing.IChangeFeedObserverFactory observerFactory;
        private readonly IChangeFeedDocumentClient documentClient;
        private readonly string collectionSelfLink;
        private readonly ILeaseManager leaseManager;
        private readonly ChangeFeedHostOptions changeFeedHostOptions;
        private readonly ChangeFeedOptions changeFeedOptions;

        public PartitionSupervisorFactory(
            FeedProcessing.IChangeFeedObserverFactory observerFactory,
            IChangeFeedDocumentClient documentClient,
            string collectionSelfLink,
            ILeaseManager leaseManager,
            ChangeFeedHostOptions options,
            ChangeFeedOptions changeFeedOptions)
        {
            if (observerFactory == null) throw new ArgumentNullException(nameof(observerFactory));
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            if (collectionSelfLink == null) throw new ArgumentNullException(nameof(collectionSelfLink));
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (changeFeedOptions == null) throw new ArgumentNullException(nameof(changeFeedOptions));

            this.observerFactory = observerFactory;
            this.documentClient = documentClient;
            this.collectionSelfLink = collectionSelfLink;
            this.leaseManager = leaseManager;
            this.changeFeedHostOptions = options;
            this.changeFeedOptions = changeFeedOptions;
        }

        public IPartitionSupervisor Create(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            var processorSettings = new ProcessorSettings
            {
                CollectionSelfLink = this.collectionSelfLink,
                RequestContinuation = lease.ContinuationToken,
                PartitionKeyRangeId = lease.PartitionId,
                FeedPollDelay = this.changeFeedHostOptions.FeedPollDelay,
                MaxItemCount = this.changeFeedHostOptions.QueryPartitionsMaxBatchSize,
                StartFromBeginning = this.changeFeedOptions.StartFromBeginning,
                StartTime = this.changeFeedOptions.StartTime,
                SessionToken = this.changeFeedOptions.SessionToken,
            };

            var checkpointer = new PartitionCheckpointer(this.leaseManager, lease);
            FeedProcessing.IChangeFeedObserver changeFeedObserver = this.observerFactory.CreateObserver();
            var processor = new PartitionProcessor(changeFeedObserver, this.documentClient, processorSettings, checkpointer);
            var renewer = new LeaseRenewer(lease, this.leaseManager, this.changeFeedHostOptions.LeaseRenewInterval);

            return new PartitionSupervisor(lease, changeFeedObserver, processor, renewer);
        }
    }
}