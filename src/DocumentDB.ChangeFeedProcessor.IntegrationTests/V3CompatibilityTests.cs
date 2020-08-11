//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Newtonsoft.Json.Linq;
    using Xunit;

    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public class V3CompatibilityTests : IntegrationTest
    {
        public V3CompatibilityTests() : base()
        {
        }

        /// <summary>
        /// Default V2 schema has PartitionId, not LeaseToken.
        /// </summary>
        [Fact]
        public async Task Schema_DefaultsToNoLeaseToken()
        {
            TestObserverFactory observerFactory = new TestObserverFactory(
                (FeedProcessing.IChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
                {
                    return Task.CompletedTask;
                });

            IChangeFeedProcessor changeFeedProcessorBuilder = await new ChangeFeedProcessorBuilder()
                    .WithObserverFactory(observerFactory)
                    .WithHostName("smoke_test")
                    .WithFeedCollection(this.MonitoredCollectionInfo)
                    .WithLeaseCollection(this.LeaseCollectionInfo)
                    .BuildAsync();

            await changeFeedProcessorBuilder.StartAsync();
            await this.WaitUntilLeaseStoreIsInitializedAsync();
            await changeFeedProcessorBuilder.StopAsync();

            // Verify that no leases have LeaseToken (V3 contract)
            int leasesProcessed = 0;
            using (DocumentClient client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
            {
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.LeaseCollectionInfo.DatabaseName, this.LeaseCollectionInfo.CollectionName);

                IDocumentQuery<JObject> query = client.CreateDocumentQuery<JObject>(collectionUri, "SELECT * FROM c").AsDocumentQuery();
                while (query.HasMoreResults)
                {
                    foreach (JObject lease in await query.ExecuteNextAsync())
                    {
                        string leaseId = lease.Value<string>("id");
                        if (leaseId.Contains(".info"))
                        {
                            // These are the store initialization marks
                            continue;
                        }

                        Assert.NotNull(lease.Value<string>("PartitionId"));
                        Assert.Null(lease.Value<string>("LeaseToken"));
                        leasesProcessed++;
                    }
                }
            }

            Assert.True(leasesProcessed > 0);
        }

        /// <summary>
        /// When the user migrates from V3 CFP, the leases contain LeaseToken.
        /// To allow for backward compatibility (V2 -> V3 -> V2) we need to honor the existence of LeaseToken and maintain its value in case the lease was created by V3.
        /// </summary>
        [Fact]
        public async Task Schema_OnV2MigrationMaintainLeaseToken()
        {
            List<int> expectedIds = Enumerable.Range(0, 20).ToList();
            ManualResetEvent firstSetOfResultsProcessed = new ManualResetEvent(false);
            ManualResetEvent secondSetOfResultsProcessed = new ManualResetEvent(false);
            List<int> receivedIds = new List<int>();
            TestObserverFactory observerFactory = new TestObserverFactory(
                (FeedProcessing.IChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
                {
                    foreach (Document doc in docs)
                    {
                        receivedIds.Add(int.Parse(doc.Id));
                    }

                    if (receivedIds.Count == 10)
                    {
                        firstSetOfResultsProcessed.Set();
                    }

                    if (receivedIds.Count == 20)
                    {
                        secondSetOfResultsProcessed.Set();
                    }

                    return Task.CompletedTask;
                });

            IChangeFeedProcessor changeFeedProcessorBuilder = await new ChangeFeedProcessorBuilder()
                    .WithObserverFactory(observerFactory)
                    .WithHostName("smoke_test")
                    .WithFeedCollection(this.MonitoredCollectionInfo)
                    .WithLeaseCollection(this.LeaseCollectionInfo)
                    .BuildAsync();

            await changeFeedProcessorBuilder.StartAsync();
            await this.WaitUntilLeaseStoreIsInitializedAsync();

            // Inserting some documents
            using (DocumentClient client = new DocumentClient(this.MonitoredCollectionInfo.Uri, this.MonitoredCollectionInfo.MasterKey, this.MonitoredCollectionInfo.ConnectionPolicy))
            {
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.MonitoredCollectionInfo.DatabaseName, this.MonitoredCollectionInfo.CollectionName);

                foreach (int id in expectedIds.Take(10))
                {
                    await client.CreateDocumentAsync(collectionUri, new { id = id.ToString() });
                }
            }

            Assert.True(firstSetOfResultsProcessed.WaitOne(IntegrationTest.changeWaitTimeout), "Timed out waiting for first set of items to be received.");

            await changeFeedProcessorBuilder.StopAsync();

            // At this point we have leases for V2, so we will simulate V3 by manually adding LeaseToken and removing PartitionId
            using (DocumentClient client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
            {
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.LeaseCollectionInfo.DatabaseName, this.LeaseCollectionInfo.CollectionName);

                IDocumentQuery<JObject> query = client.CreateDocumentQuery<JObject>(collectionUri, "SELECT * FROM c").AsDocumentQuery();
                while (query.HasMoreResults)
                {
                    foreach (JObject lease in await query.ExecuteNextAsync())
                    {
                        string leaseId = lease.Value<string>("id");
                        if (leaseId.Contains(".info"))
                        {
                            // These are the store initialization marks
                            continue;
                        }

                        // create the LeaseToken property
                        lease.Add("LeaseToken", lease.Value<string>("PartitionId"));

                        lease.Remove("PartitionId");

                        await client.UpsertDocumentAsync(collectionUri, lease);
                    }
                }
            }

            // Now all leases are V3 leases, start another processor that should migrate to V2 schema and maintain LeaseToken for compatibility

            changeFeedProcessorBuilder = await new ChangeFeedProcessorBuilder()
                    .WithObserverFactory(observerFactory)
                    .WithHostName("smoke_test")
                    .WithFeedCollection(this.MonitoredCollectionInfo)
                    .WithLeaseCollection(this.LeaseCollectionInfo)
                    .BuildAsync();

            await changeFeedProcessorBuilder.StartAsync();
            
            // Create the rest of the documents
            using (DocumentClient client = new DocumentClient(this.MonitoredCollectionInfo.Uri, this.MonitoredCollectionInfo.MasterKey, this.MonitoredCollectionInfo.ConnectionPolicy))
            {
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.MonitoredCollectionInfo.DatabaseName, this.MonitoredCollectionInfo.CollectionName);

                foreach (int id in expectedIds.TakeLast(10))
                {
                    await client.CreateDocumentAsync(collectionUri, new { id = id.ToString() });
                }
            }

            Assert.True(secondSetOfResultsProcessed.WaitOne(IntegrationTest.changeWaitTimeout), "Timed out waiting for second set of items to be received.");
            await changeFeedProcessorBuilder.StopAsync();

            // Verify we processed all items (including when using the V3 leases)
            Assert.True(!expectedIds.Except(receivedIds).Any() && expectedIds.Count == expectedIds.Count);


            // Verify the after-migration leases have both PartitionId and LeaseToken with the same value
            using (DocumentClient client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
            {
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.LeaseCollectionInfo.DatabaseName, this.LeaseCollectionInfo.CollectionName);

                IDocumentQuery<JObject> query = client.CreateDocumentQuery<JObject>(collectionUri, "SELECT * FROM c").AsDocumentQuery();
                while (query.HasMoreResults)
                {
                    foreach (JObject lease in await query.ExecuteNextAsync())
                    {
                        string leaseId = lease.Value<string>("id");
                        if (leaseId.Contains(".info"))
                        {
                            // These are the store initialization marks
                            continue;
                        }

                        Assert.NotNull(lease.Value<string>("PartitionId"));
                        Assert.Null(lease.Value<string>("LeaseToken"));
                    }
                }
            }
        }

        private async Task WaitUntilLeaseStoreIsInitializedAsync()
        {
            bool infoExists = false;
            bool lockExists = false;
            using (DocumentClient client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
            {
                Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.LeaseCollectionInfo.DatabaseName, this.LeaseCollectionInfo.CollectionName);

                IDocumentQuery<JObject> query = client.CreateDocumentQuery<JObject>(collectionUri, "SELECT * FROM c").AsDocumentQuery();
                while (query.HasMoreResults)
                {
                    foreach (JObject lease in await query.ExecuteNextAsync())
                    {
                        string leaseId = lease.Value<string>("id");
                        if (leaseId.Contains(".info"))
                        {
                            infoExists = true;
                        }

                        if (leaseId.Contains(".lock"))
                        {
                            lockExists = true;
                        }

                        if (infoExists && !lockExists)
                        {
                            return;
                        }
                    }
                }
            }
        }

        class TestObserverFactory : FeedProcessing.IChangeFeedObserverFactory, FeedProcessing.IChangeFeedObserver
        {
            private readonly Func<FeedProcessing.IChangeFeedObserverContext, IReadOnlyList<Document>, Task> changeProcessor;

            public TestObserverFactory(Func<FeedProcessing.IChangeFeedObserverContext, IReadOnlyList<Document>, Task> changeProcessor)
            {
                this.changeProcessor = changeProcessor;
            }

            public FeedProcessing.IChangeFeedObserver CreateObserver()
            {
                return this;
            }

            public Task OpenAsync(FeedProcessing.IChangeFeedObserverContext context) => Task.CompletedTask;

            public Task CloseAsync(FeedProcessing.IChangeFeedObserverContext context, FeedProcessing.ChangeFeedObserverCloseReason reason) => Task.CompletedTask;

            public Task ProcessChangesAsync(FeedProcessing.IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
            {
                if (this.changeProcessor != null) return this.changeProcessor(context, docs);
                else return Task.CompletedTask;
            }
        }
    }
}
