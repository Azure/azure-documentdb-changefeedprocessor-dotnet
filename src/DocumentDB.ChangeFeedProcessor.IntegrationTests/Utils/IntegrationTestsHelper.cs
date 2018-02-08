//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils
{
    internal class IntegrationTestsHelper
    {
        static readonly string Endpoint;
        static readonly string MasterKey;
        static readonly string DatabaseId;
        static readonly string MonitoredOfferThroughput;
        static readonly string LeaseOfferThroughput;

        static IntegrationTestsHelper()
        {
            var config = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();

            Endpoint = config["IntegrationTests:endpoint"];
            MasterKey = config["IntegrationTests:masterKey"];
            DatabaseId = config["IntegrationTests:databaseId"];
            MonitoredOfferThroughput = config["IntegrationTests:monitoredOfferThroughput"];
            LeaseOfferThroughput = config["IntegrationTests:leaseOfferThroughput"];
        }

        internal static void GetConfigurationSettings(
            out string endpoint,
            out string masterKey,
            out string databaseId)
        {
            endpoint = IntegrationTestsHelper.Endpoint;
            masterKey = IntegrationTestsHelper.MasterKey;
            databaseId = IntegrationTestsHelper.DatabaseId;
        }

        internal static void GetConfigurationSettings(
            out DocumentCollectionInfo monitoredCollectionInfo,
            out DocumentCollectionInfo leaseCollectionInfo,
            out int monitoredOfferThroughput,
            out int leaseOfferThroughput)
        {
            monitoredCollectionInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(IntegrationTestsHelper.Endpoint),
                MasterKey = IntegrationTestsHelper.MasterKey,
                DatabaseName = IntegrationTestsHelper.DatabaseId,
                ConnectionPolicy = new ConnectionPolicy { ConnectionMode = ConnectionMode.Gateway }
            };

            leaseCollectionInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(IntegrationTestsHelper.Endpoint),
                MasterKey = IntegrationTestsHelper.MasterKey,
                DatabaseName = IntegrationTestsHelper.DatabaseId,
                ConnectionPolicy = new ConnectionPolicy { ConnectionMode = ConnectionMode.Gateway }
            };

            monitoredOfferThroughput = int.Parse(IntegrationTestsHelper.MonitoredOfferThroughput);
            leaseOfferThroughput = int.Parse(IntegrationTestsHelper.LeaseOfferThroughput);
        }

        internal static async Task CreateDocumentCollectionAsync(DocumentClient client, string databaseId, DocumentCollection collection, int offerThroughput)
        {
            Debug.Assert(client != null);
            Debug.Assert(collection != null);

            var database = new Database { Id = databaseId };
            database = await client.CreateDatabaseIfNotExistsAsync(database);

            await client.CreateDocumentCollectionAsync(database.SelfLink, collection, new RequestOptions { OfferThroughput = offerThroughput });
        }

        internal static async Task CreateDocumentsAsync(DocumentClient client, Uri collectionUri, int count)
        {
            Debug.Assert(client != null);

            var dummyCounts = Enumerable.Repeat(0, count);
            var emptyDocument = new object();

            await dummyCounts.ForEachAsync(
                async dummyCounter => { await client.CreateDocumentAsync(collectionUri, emptyDocument); },
                128);
        }

        internal static async Task<int> GetPartitionCount(DocumentCollectionInfo collectionInfo)
        {
            Debug.Assert(collectionInfo != null);

            int partitionKeyRangeCount;
            using (var client = new DocumentClient(collectionInfo.Uri, collectionInfo.MasterKey, collectionInfo.ConnectionPolicy))
            {
                DocumentCollection monitoredCollection = await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(
                    collectionInfo.DatabaseName, collectionInfo.CollectionName));

                var partitionKeyRanges = await CollectionHelper.EnumPartitionKeyRangesAsync(client, monitoredCollection.SelfLink);
                partitionKeyRangeCount = partitionKeyRanges.Count;
            }

            return partitionKeyRangeCount;
        }
    }
}
