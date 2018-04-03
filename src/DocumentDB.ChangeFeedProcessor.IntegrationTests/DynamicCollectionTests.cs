//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
#pragma warning disable CS0618 // Type or member is obsolete

    /// <summary>
    /// The collection is modified while Change Feed Processor is running.
    /// </summary>
    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public class DynamicCollectionTests : IntegrationTest
    {
        const int documentCount = 513;

        public DynamicCollectionTests(IntegrationTestFixture fixture) : base(fixture, typeof(DynamicCollectionTests))
        {
        }

        [Fact]
        public async Task CountAddedDocuments()
        {
            int partitionCount = await IntegrationTestsHelper.GetPartitionCount(this.ClassData.monitoredCollectionInfo);
            int openedCount = 0, processedCount = 0;
            var allObserversStarted = new ManualResetEvent(false);
            var allDocsProcessed = new ManualResetEvent(false);

            var observerFactory = new TestObserverFactory(
                context =>
                {
                    int newCount = Interlocked.Increment(ref openedCount);
                    if (newCount == partitionCount) allObserversStarted.Set();
                    return Task.CompletedTask;
                },
                null,
                (ChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
                {
                    int newCount = Interlocked.Add(ref processedCount, docs.Count);
                    if (newCount == documentCount) allDocsProcessed.Set();
                    return Task.CompletedTask;
                });

            var host = new ChangeFeedEventHost(
                Guid.NewGuid().ToString(),
                this.ClassData.monitoredCollectionInfo,
                this.LeaseCollectionInfo,
                new ChangeFeedOptions { StartFromBeginning = false },
                new ChangeFeedHostOptions());
            await host.RegisterObserverFactoryAsync(observerFactory);

            var isStartOk = allObserversStarted.WaitOne(IntegrationTest.changeWaitTimeout + IntegrationTest.changeWaitTimeout);
            Assert.True(isStartOk, "Timed out waiting for observres to start");

            using (var client = new DocumentClient(this.ClassData.monitoredCollectionInfo.Uri, this.ClassData.monitoredCollectionInfo.MasterKey, this.ClassData.monitoredCollectionInfo.ConnectionPolicy))
            {
                await IntegrationTestsHelper.CreateDocumentsAsync(
                    client,
                    UriFactory.CreateDocumentCollectionUri(this.ClassData.monitoredCollectionInfo.DatabaseName, this.ClassData.monitoredCollectionInfo.CollectionName),
                    documentCount);
            }

            allDocsProcessed.WaitOne(IntegrationTest.changeWaitTimeout);

            try
            {
                Assert.True(documentCount == processedCount, "Wrong processedCount");
            }
            finally
            {
                await host.UnregisterObserversAsync();
            }
        }

        [Fact]
        public async Task TestStartTime()
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(this.ClassData.monitoredCollectionInfo.DatabaseName, this.ClassData.monitoredCollectionInfo.CollectionName);
            using (var client = new DocumentClient(this.ClassData.monitoredCollectionInfo.Uri, this.ClassData.monitoredCollectionInfo.MasterKey, this.ClassData.monitoredCollectionInfo.ConnectionPolicy))
            {
                await client.CreateDocumentAsync(collectionUri, JsonConvert.DeserializeObject("{\"id\": \"doc1\"}"));

                // In worst case (long transaction, heavy load, the atomicity of StartTime is 5 sec).
                // For this case (different transactions) it's OK to wait timestamp precision time.
                await Task.Delay(TimeSpan.FromSeconds(1));
                DateTime timeInBeweeen = DateTime.Now;
                await Task.Delay(TimeSpan.FromSeconds(1));

                await client.CreateDocumentAsync(collectionUri, JsonConvert.DeserializeObject("{\"id\": \"doc2\"}"));

                int partitionCount = await IntegrationTestsHelper.GetPartitionCount(this.ClassData.monitoredCollectionInfo);
                var allDocsProcessed = new ManualResetEvent(false);

                var processedDocs = new List<Document>();
                var observerFactory = new TestObserverFactory(
                    null,
                    null,
                    (context, docs) =>
                    {
                        processedDocs.AddRange(docs);
                        foreach (var doc in docs)
                        {
                            if (doc.Id == "doc2") allDocsProcessed.Set();
                        }
                        return Task.CompletedTask;
                    });

                var host = new ChangeFeedEventHost(
                    Guid.NewGuid().ToString(),
                    this.ClassData.monitoredCollectionInfo,
                    this.LeaseCollectionInfo,
                    new ChangeFeedOptions { StartTime = timeInBeweeen },
                    new ChangeFeedHostOptions());
                await host.RegisterObserverFactoryAsync(observerFactory);

                var isStartOk = allDocsProcessed.WaitOne(IntegrationTest.changeWaitTimeout + IntegrationTest.changeWaitTimeout);

                try
                {
                    Assert.True(isStartOk, "Timed out waiting for docs to process");
                    Assert.True(1 == processedDocs.Count, "Wrong processed count");
                    Assert.True("doc2" == processedDocs[0].Id, "Wrong doc.id");
                }
                finally
                {
                    await host.UnregisterObserversAsync();
                }
            }
        }

        [Fact]
        public async Task TestReducePageSizeScenario()
        {
            // Use different colleciton: we need 1-partition collection to make sure all docs get to same partition.
            var databaseUri = UriFactory.CreateDatabaseUri(this.ClassData.monitoredCollectionInfo.DatabaseName);

            DocumentCollectionInfo monitoredCollectionInfo = new DocumentCollectionInfo(this.ClassData.monitoredCollectionInfo);
            monitoredCollectionInfo.CollectionName = this.ClassData.monitoredCollectionInfo.CollectionName + "_" + Guid.NewGuid().ToString();

            var collectionUri = UriFactory.CreateDocumentCollectionUri(this.ClassData.monitoredCollectionInfo.DatabaseName, monitoredCollectionInfo.CollectionName);
            var monitoredCollection = new DocumentCollection { Id = monitoredCollectionInfo.CollectionName };

            using (var client = new DocumentClient(this.ClassData.monitoredCollectionInfo.Uri, this.ClassData.monitoredCollectionInfo.MasterKey, this.ClassData.monitoredCollectionInfo.ConnectionPolicy))
            {
                await client.CreateDocumentCollectionAsync(databaseUri, monitoredCollection, new RequestOptions { OfferThroughput = 10000 });

                try
                {
                    // Create some docs to make sure that one separate response is returned for 1st execute of query before retries.
                    // These are to make sure continuation token is passed along during retries.
                    var sproc = new StoredProcedure
                    {
                        Id = "createTwoDocs",
                        Body = @"function(startIndex) { for (var i = 0; i < 2; ++i) __.createDocument(
                            __.getSelfLink(),
                            { id: 'doc' + (i + startIndex).toString(), value: 'y'.repeat(1500000) },
                            err => { if (err) throw err;}
                        );}"
                    };

                    var sprocUri = UriFactory.CreateStoredProcedureUri(this.ClassData.monitoredCollectionInfo.DatabaseName, monitoredCollection.Id, sproc.Id);
                    await client.CreateStoredProcedureAsync(collectionUri, sproc);
                    await client.ExecuteStoredProcedureAsync<object>(sprocUri, 0);

                    // Create 3 docs each 1.5MB. All 3 do not fit into MAX_RESPONSE_SIZE (4 MB). 2nd and 3rd are in same transaction.
                    var content = string.Format("{{\"id\": \"doc2\", \"value\": \"{0}\"}}", new string('x', 1500000));
                    await client.CreateDocumentAsync(collectionUri, JsonConvert.DeserializeObject(content));
                    await client.ExecuteStoredProcedureAsync<object>(sprocUri, 3);

                    var allDocsProcessed = new ManualResetEvent(false);
                    int processedDocCount = 0;
                    string accumulator = string.Empty;

                    var observerFactory = new TestObserverFactory(
                        null,
                        null,
                        (context, docs) =>
                        {
                            processedDocCount += docs.Count;
                            foreach (var doc in docs) accumulator += doc.Id.ToString() + ".";
                            if (processedDocCount == 5) allDocsProcessed.Set();
                            return Task.CompletedTask;
                        });

                    var host = new ChangeFeedEventHost(
                        Guid.NewGuid().ToString(),
                        monitoredCollectionInfo,
                        this.LeaseCollectionInfo,
                        new ChangeFeedOptions { StartFromBeginning = true, MaxItemCount = 6 },
                        new ChangeFeedHostOptions());
                    await host.RegisterObserverFactoryAsync(observerFactory);

                    var isStartOk = allDocsProcessed.WaitOne(IntegrationTest.changeWaitTimeout + IntegrationTest.changeWaitTimeout);

                    try
                    {
                        Assert.True(isStartOk, "Timed out waiting for docs to process");
                        Assert.Equal("doc0.doc1.doc2.doc3.doc4.", accumulator);
                    }
                    finally
                    {
                        await host.UnregisterObserversAsync();
                    }
                }
                finally
                {
                    await client.DeleteDocumentCollectionAsync(collectionUri);
                }
            }
        }
    }
}
