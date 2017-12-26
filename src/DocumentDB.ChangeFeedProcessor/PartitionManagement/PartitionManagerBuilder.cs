//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public class PartitionManagerBuilder : IPartitionManagerBuilder
    {
        private ILeaseManager leaseManager;
        private DocumentCollectionInfo leaseCollectionLocation;
        private IDocumentClientEx leaseDocumentClient;
        private readonly TimeSpan sleepTime = TimeSpan.FromSeconds(15);
        private readonly TimeSpan lockTime = TimeSpan.FromSeconds(30);

        public PartitionManagerBuilder WithLeaseManager(ILeaseManager leaseManager)
        {
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            this.leaseManager = leaseManager;
            return this;
        }

        public PartitionManagerBuilder WithLeaseCollection(DocumentCollectionInfo leaseCollectionLocation)
        {
            if (leaseCollectionLocation == null) throw new ArgumentNullException(nameof(leaseCollectionLocation));
            this.leaseCollectionLocation = leaseCollectionLocation.Canonicalize();
            return this;
        }

        public PartitionManagerBuilder WithLeaseDocumentClient(IDocumentClientEx leaseDocumentClient)
        {
            if (leaseDocumentClient == null) throw new ArgumentNullException(nameof(leaseDocumentClient));
            this.leaseDocumentClient = leaseDocumentClient;
            return this;
        }

        public async Task<IChangeFeedProcessor> BuildPartitionManagerAsync(string hostName, 
                                                                        string leasePrefix, 
                                                                        IChangeFeedObserverFactory observerFactory,
                                                                        IDocumentClientEx feedDocumentClient, 
                                                                        DocumentCollectionInfo feedCollectionInfo,
                                                                        ChangeFeedOptions feedOptions, 
                                                                        ChangeFeedHostOptions options)
        {
            if (leaseCollectionLocation == null) throw new InvalidOperationException(nameof(leaseCollectionLocation) + " was not specified");
            leaseDocumentClient = leaseDocumentClient ?? leaseCollectionLocation.CreateDocumentClient();

            DocumentCollection documentCollection = await leaseDocumentClient.GetDocumentCollectionAsync(leaseCollectionLocation).ConfigureAwait(false);
            string leaseStoreCollectionLink = documentCollection.SelfLink;

            if (this.leaseManager == null)
            {
                this.leaseManager = CreateLeaseManager(leasePrefix, leaseStoreCollectionLink);
            }

            string collectionSelfLink = feedCollectionInfo.GetCollectionSelfLink();
            IChangeFeedObserverFactory factory = new CheckpointerObserverFactory(observerFactory, options.CheckpointFrequency);
            var synchronizer = new PartitionSynchronizer(feedDocumentClient, collectionSelfLink, leaseManager, options.DegreeOfParallelism, options.QueryPartitionsMaxBatchSize);
            var leaseStore = new LeaseStore(leaseDocumentClient, leaseCollectionLocation, leasePrefix, leaseStoreCollectionLink);
            var bootstrapper = new Bootstrapper(synchronizer, leaseStore, lockTime, sleepTime);
            var partitionObserverFactory = new PartitionSupervisorFactory(factory, feedDocumentClient, collectionSelfLink, leaseManager, options, feedOptions);
            var partitionController = new PartitionController(hostName, leaseManager, partitionObserverFactory, synchronizer);
            var partitionEstimator = await this.BuildRemainingWorkEstimatorAsync(leasePrefix, feedDocumentClient, feedCollectionInfo).ConfigureAwait(false);
            var loadBalancingStrategy = new EqualPartitionsBalancingStrategy(hostName, options.MinPartitionCount, options.MaxPartitionCount, options.LeaseExpirationInterval);
            var partitionLoadBalancer = new PartitionLoadBalancer(partitionController, leaseManager, loadBalancingStrategy, options.LeaseAcquireInterval);
            return new PartitionManager(bootstrapper, partitionController, partitionLoadBalancer, partitionEstimator);
        }

        public async Task<IRemainingWorkEstimator> BuildRemainingWorkEstimatorAsync(string leasePrefix,
                                                                        IDocumentClientEx feedDocumentClient,
                                                                        DocumentCollectionInfo feedCollectionInfo)
        {
            if (this.leaseManager == null)
            {
                if (this.leaseCollectionLocation == null)
                {
                    throw new InvalidOperationException(nameof(this.leaseCollectionLocation) + " was not specified");
                }

                this.leaseDocumentClient = this.leaseDocumentClient ?? this.leaseCollectionLocation.CreateDocumentClient();

                DocumentCollection documentCollection = await this.leaseDocumentClient.GetDocumentCollectionAsync(this.leaseCollectionLocation).ConfigureAwait(false);
                string leaseStoreCollectionLink = documentCollection.SelfLink;

                this.leaseManager = this.CreateLeaseManager(leasePrefix, leaseStoreCollectionLink);
            }

            string collectionSelfLink = feedCollectionInfo.GetCollectionSelfLink();
            return new RemainingWorkEstimator(this.leaseManager, feedDocumentClient, collectionSelfLink);
        }

        private DocumentServiceLeaseManager CreateLeaseManager(string leasePrefix, string leaseStoreCollectionLink)
        {
            var updater = new DocumentServiceLeaseUpdater(leaseDocumentClient);
            var documentServiceLeaseManager = new DocumentServiceLeaseManager(
                leaseDocumentClient,
                updater,
                leaseCollectionLocation,
                leasePrefix,
                leaseStoreCollectionLink);
            return documentServiceLeaseManager;
        }
    }
}