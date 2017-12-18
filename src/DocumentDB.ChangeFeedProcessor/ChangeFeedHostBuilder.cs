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

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    public class ChangeFeedHostBuilder
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private string hostName;
        private DocumentCollectionInfo feedCollectionLocation;
        private ChangeFeedHostOptions changeFeedHostOptions;
        private IDocumentClientEx feedDocumentClient;
        private IChangeFeedObserverFactory observerFactory;
        private string databaseResourceId;
        private string collectionResourceId;
        private IPartitionManagerBuilder partitionManagerBuilder;

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

        public ChangeFeedHostBuilder WithPartitionManagerBuilder(IPartitionManagerBuilder partitionManagerBuilder)
        {
            if (partitionManagerBuilder == null) throw new ArgumentNullException(nameof(partitionManagerBuilder));
            this.partitionManagerBuilder = partitionManagerBuilder;
            return this;
        }

        public async Task<IChangeFeedHost> BuildAsync()
        {
            if (hostName == null) throw new InvalidOperationException("Host name was not specified");
            if (feedCollectionLocation == null) throw new InvalidOperationException(nameof(feedCollectionLocation) + " was not specified");
            if (observerFactory == null) throw new InvalidOperationException("Observer was not specified");

            feedDocumentClient = feedDocumentClient ?? feedCollectionLocation.CreateDocumentClient();
            changeFeedHostOptions = changeFeedHostOptions ?? new ChangeFeedHostOptions();
            databaseResourceId = databaseResourceId ?? await GetDatabaseResourceIdAsync(feedDocumentClient, feedCollectionLocation).ConfigureAwait(false);
            collectionResourceId = collectionResourceId ?? await GetCollectionResourceIdAsync(feedDocumentClient, feedCollectionLocation).ConfigureAwait(false);
            partitionManagerBuilder = partitionManagerBuilder ?? new PartitionManagerBuilder();

            string optionsPrefix = changeFeedHostOptions.LeasePrefix ?? string.Empty;
            string leasePrefix = string.Format(CultureInfo.InvariantCulture, "{0}{1}_{2}_{3}", optionsPrefix, feedCollectionLocation.Uri.Host, databaseResourceId, collectionResourceId);

            IPartitionManager partitionManager = await partitionManagerBuilder.BuildPartitionManagerAsync(hostName, leasePrefix, observerFactory,
                feedDocumentClient, feedCollectionLocation, changeFeedHostOptions).ConfigureAwait(false);
            return new ChangeFeedHost(partitionManager);
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
    }
}