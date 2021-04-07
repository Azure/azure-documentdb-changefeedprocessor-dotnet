//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils;    
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;

    /// <summary>
    /// Test ChangeFeed with Custom Partition key lease collections
    /// </summary>
    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public class CustomPartitionedLeaseCollectionTests:IntegrationTest
    {
        const int documentCount = 500;
        public CustomPartitionedLeaseCollectionTests():base(isPartitionedMonitoredCollection:true, isPartitionedLeaseCollection:true,leaseCollectionPartitionKey:"leaseId")
        {
        }

        [Fact]        
        public async Task CountDocumentsInCollection_TwoHosts()
        {
            await this.InitializeDocumentsAsync();
            int partitionKeyRangeCount = await IntegrationTestsHelper.GetPartitionCount(this.MonitoredCollectionInfo);
            Assert.True(partitionKeyRangeCount > 1, "Prerequisite failed: expected monitored collection with at least 2 partitions.");

            int processedCount = 0;
            var allDocsProcessed = new ManualResetEvent(false);

            var observerFactory = new V2TestObserverFactory(
                openProcessor: null,
                changeProcessor:(FeedProcessing.IChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
                {
                    int newCount = Interlocked.Add(ref processedCount, docs.Count);
                    if (newCount == documentCount)
                    {
                        allDocsProcessed.Set();
                    }
                    return Task.CompletedTask;
                });

            var host1 = await new ChangeFeedProcessorBuilder()
                  .WithObserverFactory(observerFactory)
                  .WithHostName(Guid.NewGuid().ToString())
                  .WithFeedCollection(this.MonitoredCollectionInfo)
                  .WithLeaseCollection(this.LeaseCollectionInfo)
                  .WithLeaseCollectionPartitionKeyName("leaseId")
                  .WithProcessorOptions(new ChangeFeedProcessorOptions()
                  {
                      StartFromBeginning = true                      
                  })
                  .BuildAsync();
            await host1.StartAsync();
            var host2 = await new ChangeFeedProcessorBuilder()
                  .WithObserverFactory(observerFactory)
                  .WithHostName(Guid.NewGuid().ToString())
                  .WithFeedCollection(this.MonitoredCollectionInfo)
                  .WithLeaseCollection(this.LeaseCollectionInfo)
                  .WithLeaseCollectionPartitionKeyName("leaseId")
                  .WithProcessorOptions(new ChangeFeedProcessorOptions()
                   {
                       StartFromBeginning = true
                   })
                  .BuildAsync();
            await host2.StartAsync();
            await this.WaitUntilLeaseStoreIsInitializedAsync(new CancellationTokenSource(5000).Token);
            allDocsProcessed.WaitOne(changeWaitTimeout + changeWaitTimeout);

            try
            {
                Assert.True(documentCount == processedCount, $"Wrong processedCount {documentCount} {processedCount}");
            }
            finally
            {
                await host1.StopAsync();
                await host2.StopAsync();
            }
        }

        private async Task InitializeDocumentsAsync()
        {
            using (var client = new DocumentClient(this.MonitoredCollectionInfo.Uri, this.MonitoredCollectionInfo.MasterKey, this.MonitoredCollectionInfo.ConnectionPolicy))
            {
                var collectionUri = UriFactory.CreateDocumentCollectionUri(this.MonitoredCollectionInfo.DatabaseName, this.MonitoredCollectionInfo.CollectionName);

                await IntegrationTestsHelper.CreateDocumentsAsync(client, collectionUri, documentCount);
            }
        }

        private async Task WaitUntilLeaseStoreIsInitializedAsync(CancellationToken cancellationToken)
        {
            bool infoExists = false;
            bool lockExists = false;
            while (true)
            {
                infoExists = false;
                lockExists = false;
                cancellationToken.ThrowIfCancellationRequested();
                using (DocumentClient client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
                {
                    Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.LeaseCollectionInfo.DatabaseName, this.LeaseCollectionInfo.CollectionName);

                    IDocumentQuery<JObject> query = client.CreateDocumentQuery<JObject>(collectionUri, "SELECT * FROM c WHERE CONTAINS(c.id, \".info\") OR CONTAINS(c.id, \".lock\")",new FeedOptions() { EnableCrossPartitionQuery = true }).AsDocumentQuery();
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
                        }
                    }
                }

                if (infoExists && !lockExists)
                {
                    return;
                }

                await Task.Delay(100, cancellationToken);
            }
        }
    }
}
