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
        private readonly IChangeFeedObserverFactory observerFactory;
        private readonly IChangeFeedDocumentClient documentClient;
        private readonly string collectionSelfLink;
        private readonly ILeaseManager leaseManager;
        private readonly ChangeFeedHostOptions changeFeedHostOptions;
        private readonly ChangeFeedOptions changeFeedOptions;
        private readonly IPartitionProcessorFactory partitionProcessorFactory;

        public PartitionSupervisorFactory(
            IChangeFeedObserverFactory observerFactory,
            IChangeFeedDocumentClient documentClient,
            string collectionSelfLink,
            ILeaseManager leaseManager,
            IPartitionProcessorFactory partitionProcessorFactory,
            ChangeFeedHostOptions options,
            ChangeFeedOptions changeFeedOptions)
        {
            if (observerFactory == null) throw new ArgumentNullException(nameof(observerFactory));
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            if (collectionSelfLink == null) throw new ArgumentNullException(nameof(collectionSelfLink));
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (changeFeedOptions == null) throw new ArgumentNullException(nameof(changeFeedOptions));
            if (partitionProcessorFactory == null) throw new ArgumentNullException(nameof(partitionProcessorFactory));

            this.observerFactory = observerFactory;
            this.documentClient = documentClient;
            this.collectionSelfLink = collectionSelfLink;
            this.leaseManager = leaseManager;
            this.changeFeedHostOptions = options;
            this.changeFeedOptions = changeFeedOptions;
            this.partitionProcessorFactory = partitionProcessorFactory;
        }

        public IPartitionSupervisor Create(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            IChangeFeedObserver changeFeedObserver = this.observerFactory.CreateObserver();
            var processor = this.partitionProcessorFactory.Create(changeFeedObserver, lease);
            var renewer = new LeaseRenewer(lease, this.leaseManager, this.changeFeedHostOptions.LeaseRenewInterval);

            return new PartitionSupervisor(lease, changeFeedObserver, processor, renewer);
        }
    }
}