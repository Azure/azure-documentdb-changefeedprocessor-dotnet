//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal class PartitionSupervisorFactory : IPartitionSupervisorFactory
    {
        private readonly IChangeFeedObserverFactory observerFactory;
        private readonly IDocumentClientEx documentClient;
        private readonly string collectionSelfLink;
        private readonly ILeaseManager leaseManager;
        private readonly ChangeFeedHostOptions changeFeedHostOptions;
        private readonly ChangeFeedOptions changeFeedOptions;

        public PartitionSupervisorFactory(IChangeFeedObserverFactory observerFactory,
                                          IDocumentClientEx documentClient,
                                          string collectionSelfLink,
                                          ILeaseManager leaseManager,
                                          ChangeFeedOptions options,
                                          ChangeFeedHostOptions hostOptions)
        {
            if (observerFactory == null) throw new ArgumentNullException(nameof(observerFactory));
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            if (collectionSelfLink == null) throw new ArgumentNullException(nameof(collectionSelfLink));
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (hostOptions == null) throw new ArgumentNullException(nameof(hostOptions));

            this.observerFactory = observerFactory;
            this.documentClient = documentClient;
            this.collectionSelfLink = collectionSelfLink;
            this.leaseManager = leaseManager;
            this.changeFeedHostOptions = hostOptions;
            this.changeFeedOptions = options;
        }

        public IPartitionSupervisor Create(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            ChangeFeedOptions options = new ChangeFeedOptions
            {
                MaxItemCount = this.changeFeedOptions.MaxItemCount.HasValue? this.changeFeedOptions.MaxItemCount.Value : changeFeedHostOptions.QueryPartitionsMaxBatchSize,
                SessionToken = this.changeFeedOptions.SessionToken,
                StartFromBeginning = this.changeFeedOptions.StartFromBeginning,
                RequestContinuation = lease.ContinuationToken,
                PartitionKeyRangeId = lease.PartitionId
            };

            var checkpointer = new PartitionCheckpointer(leaseManager, lease);
            IChangeFeedObserver changeFeedObserver = observerFactory.CreateObserver();
            var processor = new PartitionProcessor(changeFeedObserver, documentClient, collectionSelfLink, changeFeedOptions, changeFeedHostOptions, checkpointer);
            var renewer = new LeaseRenewer(lease, leaseManager, changeFeedHostOptions.LeaseRenewInterval);

            return new PartitionSupervisor(lease, changeFeedObserver, processor, renewer);
        }
    }
}