//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Processing;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using Client;

    internal class PartitionSupervisorFactory : IPartitionSupervisorFactory
    {
        private readonly Processing.IChangeFeedObserverFactory observerFactory;
        private readonly IChangeFeedDocumentClient documentClient;
        private readonly string collectionSelfLink;
        private readonly ILeaseManager leaseManager;
        private readonly ChangeFeedHostOptions changeFeedHostOptions;
        private readonly ChangeFeedOptions changeFeedOptions;

        public PartitionSupervisorFactory(
            Processing.IChangeFeedObserverFactory observerFactory,
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
            changeFeedHostOptions = options;
            this.changeFeedOptions = changeFeedOptions;
        }

        public IPartitionSupervisor Create(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            var processorSettings = new ProcessorSettings
            {
                CollectionSelfLink = collectionSelfLink,
                RequestContinuation = lease.ContinuationToken,
                PartitionKeyRangeId = lease.PartitionId,
                FeedPollDelay = changeFeedHostOptions.FeedPollDelay,
                MaxItemCount = changeFeedHostOptions.QueryPartitionsMaxBatchSize,
                StartFromBeginning = changeFeedOptions.StartFromBeginning,
                StartTime = changeFeedOptions.StartTime,
                SessionToken = changeFeedOptions.SessionToken,
            };

            var checkpointer = new PartitionCheckpointer(leaseManager, lease);
            Processing.IChangeFeedObserver changeFeedObserver = observerFactory.CreateObserver();
            var processor = new PartitionProcessor(changeFeedObserver, documentClient, processorSettings, checkpointer);
            var renewer = new LeaseRenewer(lease, leaseManager, changeFeedHostOptions.LeaseRenewInterval);

            return new PartitionSupervisor(lease, changeFeedObserver, processor, renewer);
        }
    }
}