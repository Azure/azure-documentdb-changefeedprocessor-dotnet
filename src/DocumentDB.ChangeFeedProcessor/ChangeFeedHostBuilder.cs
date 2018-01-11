//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    public class ChangeFeedHostBuilder
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private string hostName;
        private DocumentCollectionInfo feedCollectionLocation;
        private ChangeFeedHostOptions changeFeedHostOptions;
        private ChangeFeedOptions changeFeedOptions;
        private IDocumentClientEx feedDocumentClient;
        private IChangeFeedObserverFactory observerFactory;
        private string databaseResourceId;
        private string collectionResourceId;
        private DocumentCollectionInfo leaseCollectionLocation;
        private IDocumentClientEx leaseDocumentClient;
        private ILeaseManager leaseManager;
        private readonly TimeSpan sleepTime = TimeSpan.FromSeconds(15);
        private readonly TimeSpan lockTime = TimeSpan.FromSeconds(30);

        public ChangeFeedHostBuilder WithHostName(string hostName)
        {
            this.hostName = hostName;
            return this;
        }

        public ChangeFeedHostBuilder WithFeedCollection(DocumentCollectionInfo feedCollectionLocation)
        {
            if (feedCollectionLocation == null) throw new ArgumentNullException(nameof(feedCollectionLocation));
            this.feedCollectionLocation = feedCollectionLocation.Canonicalize();
            return this;
        }

        public ChangeFeedHostBuilder WithFeedDocumentClient(IDocumentClientEx feedDocumentClient)
        {
            if (feedDocumentClient == null) throw new ArgumentNullException(nameof(feedDocumentClient));
            this.feedDocumentClient = feedDocumentClient;
            return this;
        }

        public ChangeFeedHostBuilder WithChangeFeedHostOptions(ChangeFeedHostOptions changeFeedHostOptions)
        {
            if (changeFeedHostOptions == null) throw new ArgumentNullException(nameof(changeFeedHostOptions));
            this.changeFeedHostOptions = changeFeedHostOptions;
            return this;
        }

        public ChangeFeedHostBuilder WithChangeFeedOptions(ChangeFeedOptions changeFeedOptions)
        {
            if (changeFeedOptions == null) throw new ArgumentNullException(nameof(changeFeedOptions));
            this.changeFeedOptions = changeFeedOptions;
            return this;
        }

        public ChangeFeedHostBuilder WithObserverFactory(IChangeFeedObserverFactory observerFactory)
        {
            if (observerFactory == null) throw new ArgumentNullException(nameof(observerFactory));
            this.observerFactory = observerFactory;
            return this;
        }

        public ChangeFeedHostBuilder WithObserver<T>() where T : IChangeFeedObserver, new()
        {
            this.observerFactory = new ChangeFeedObserverFactory<T>();
            return this;
        }

        public ChangeFeedHostBuilder WithDatabaseResourceId(string databaseResourceId)
        {
            if (databaseResourceId == null) throw new ArgumentNullException(nameof(databaseResourceId));
            this.databaseResourceId = databaseResourceId;
            return this;
        }

        public ChangeFeedHostBuilder WithCollectionResourceId(string collectionResourceId)
        {
            if (collectionResourceId == null) throw new ArgumentNullException(nameof(collectionResourceId));
            this.collectionResourceId = collectionResourceId;
            return this;
        }

        public ChangeFeedHostBuilder WithLeaseCollection(DocumentCollectionInfo leaseCollectionLocation)
        {
            if (leaseCollectionLocation == null) throw new ArgumentNullException(nameof(leaseCollectionLocation));
            this.leaseCollectionLocation = leaseCollectionLocation.Canonicalize();
            return this;
        }

        public ChangeFeedHostBuilder WithLeaseDocumentClient(IDocumentClientEx leaseDocumentClient)
        {
            if (leaseDocumentClient == null) throw new ArgumentNullException(nameof(leaseDocumentClient));
            this.leaseDocumentClient = leaseDocumentClient;
            return this;
        }

        public ChangeFeedHostBuilder WithLeaseManager(ILeaseManager leaseManager)
        {
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            this.leaseManager = leaseManager;
            return this;
        }

        public async Task<IChangeFeedProcessor> BuildAsync()
        {
            if (this.hostName == null)
            {
                throw new InvalidOperationException("Host name was not specified");
            }

            if (this.feedCollectionLocation == null)
            {
                throw new InvalidOperationException(nameof(this.feedCollectionLocation) + " was not specified");
            }

            if (this.leaseCollectionLocation == null)
            {
                throw new InvalidOperationException(nameof(this.leaseCollectionLocation) + " was not specified");
            }

            if (this.observerFactory == null)
            {
                throw new InvalidOperationException("Observer was not specified");
            }

            await this.InitializeCollectionPropertiesForBuildAsync().ConfigureAwait(false);

            ILeaseManager leaseManager = await this.GetLeaseManagerAsync().ConfigureAwait(false);

            IPartitionManager partitionManager = await this.BuildPartitionManagerAsync(leaseManager).ConfigureAwait(false);
            IRemainingWorkEstimator remainingWorkEstimator = new RemainingWorkEstimator(leaseManager, this.feedDocumentClient, this.feedCollectionLocation.GetCollectionSelfLink());
            return new ChangeFeedHost(partitionManager, remainingWorkEstimator);
        }

        internal async Task<IChangeFeedProcessor> BuildEstimatorAsync()
        {
            if (this.feedCollectionLocation == null)
            {
                throw new InvalidOperationException(nameof(this.feedCollectionLocation) + " was not specified");
            }

            if (this.leaseCollectionLocation == null)
            {
                throw new InvalidOperationException(nameof(this.leaseCollectionLocation) + " was not specified");
            }

            await this.InitializeCollectionPropertiesForBuildAsync().ConfigureAwait(false);

            ILeaseManager leaseManager = await this.GetLeaseManagerAsync().ConfigureAwait(false);

            IRemainingWorkEstimator remainingWorkEstimator = new RemainingWorkEstimator(leaseManager, this.feedDocumentClient, this.feedCollectionLocation.GetCollectionSelfLink());
            return new ChangeFeedEstimateHost(remainingWorkEstimator);
        }

        private static async Task<string> GetDatabaseResourceIdAsync(IDocumentClientEx documentClient, DocumentCollectionInfo collectionLocation)
        {
            logger.InfoFormat("Reading database: '{0}'", collectionLocation.DatabaseName);
            Uri databaseUri = UriFactory.CreateDatabaseUri(collectionLocation.DatabaseName);
            Database database = await documentClient.ReadDatabaseAsync(databaseUri, null).ConfigureAwait(false);
            return database.ResourceId;
        }

        private static async Task<string> GetCollectionResourceIdAsync(IDocumentClientEx documentClient, DocumentCollectionInfo collectionLocation)
        {
            logger.InfoFormat("Reading collection: '{0}'", collectionLocation.CollectionName);
            DocumentCollection documentCollection = await documentClient.GetDocumentCollectionAsync(collectionLocation).ConfigureAwait(false);
            return documentCollection.ResourceId;
        }

        private async Task<IPartitionManager> BuildPartitionManagerAsync(ILeaseManager leaseManager)
        {
            this.leaseDocumentClient = this.leaseDocumentClient ?? this.leaseCollectionLocation.CreateDocumentClient();

            DocumentCollection documentCollection = await this.leaseDocumentClient.GetDocumentCollectionAsync(this.leaseCollectionLocation).ConfigureAwait(false);
            string leaseStoreCollectionLink = documentCollection.SelfLink;

            string collectionSelfLink = this.feedCollectionLocation.GetCollectionSelfLink();
            IChangeFeedObserverFactory factory = new CheckpointerObserverFactory(this.observerFactory, this.changeFeedHostOptions.CheckpointFrequency);
            var synchronizer = new PartitionSynchronizer(this.feedDocumentClient, collectionSelfLink, leaseManager, this.changeFeedHostOptions.DegreeOfParallelism, this.changeFeedHostOptions.QueryPartitionsMaxBatchSize);
            var leaseStore = new LeaseStore(this.leaseDocumentClient, this.leaseCollectionLocation, this.GetLeasePrefix(), leaseStoreCollectionLink);
            var bootstrapper = new Bootstrapper(synchronizer, leaseStore, this.lockTime, this.sleepTime);
            var partitionObserverFactory = new PartitionSupervisorFactory(factory, this.feedDocumentClient, collectionSelfLink, leaseManager, this.changeFeedHostOptions, this.changeFeedOptions);
            var partitionController = new PartitionController(this.hostName, leaseManager, partitionObserverFactory, synchronizer);
            var loadBalancingStrategy = new EqualPartitionsBalancingStrategy(this.hostName, this.changeFeedHostOptions.MinPartitionCount, this.changeFeedHostOptions.MaxPartitionCount, this.changeFeedHostOptions.LeaseExpirationInterval);
            var partitionLoadBalancer = new PartitionLoadBalancer(partitionController, leaseManager, loadBalancingStrategy, this.changeFeedHostOptions.LeaseAcquireInterval);
            return new PartitionManager(bootstrapper, partitionController, partitionLoadBalancer);
        }

        private async Task<ILeaseManager> GetLeaseManagerAsync()
        {

            if (this.leaseManager == null)
            {
                string leasePrefix = this.GetLeasePrefix();
                var leaseManagerBuilder = new LeaseManagerBuilder()
                    .WithLeasePrefix(leasePrefix)
                    .WithLeaseCollection(this.leaseCollectionLocation);
                this.leaseManager = await leaseManagerBuilder.BuildAsync().ConfigureAwait(false);
            }

            return this.leaseManager;
        }

        private string GetLeasePrefix()
        {
            string optionsPrefix = this.changeFeedHostOptions.LeasePrefix ?? string.Empty;
            return string.Format(CultureInfo.InvariantCulture, "{0}{1}_{2}_{3}", optionsPrefix, this.feedCollectionLocation.Uri.Host, this.databaseResourceId, this.collectionResourceId);
        }



        private async Task InitializeCollectionPropertiesForBuildAsync()
        {
            this.feedDocumentClient = this.feedDocumentClient ?? this.feedCollectionLocation.CreateDocumentClient();
            this.changeFeedHostOptions = this.changeFeedHostOptions ?? new ChangeFeedHostOptions();
            this.changeFeedOptions = this.changeFeedOptions ?? new ChangeFeedOptions();
            this.databaseResourceId = this.databaseResourceId ?? await GetDatabaseResourceIdAsync(this.feedDocumentClient, this.feedCollectionLocation).ConfigureAwait(false);
            this.collectionResourceId = this.collectionResourceId ?? await GetCollectionResourceIdAsync(this.feedDocumentClient, this.feedCollectionLocation).ConfigureAwait(false);
        }
    }
}