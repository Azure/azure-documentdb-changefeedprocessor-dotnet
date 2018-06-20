using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.Client;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
    public class SmokeTest
    {
        [Fact]
        public async Task WriteToDocDB_SouldGeneratesIncomingEvent_IfStartingFromDefaultPoint()
        {
            string sessionId = Guid.NewGuid().ToString();

            IntegrationTestsHelper.GetConfigurationSettings(out DocumentCollectionInfo feedCollectionInfo, out DocumentCollectionInfo leaseCollectionInfo, out int feedOfferThroughput, out int leaseOfferThroughput);
            feedCollectionInfo.CollectionName = Guid.NewGuid().ToString();
            leaseCollectionInfo.CollectionName = feedCollectionInfo.CollectionName + "-lease";

            var feedClient = new DocumentClient(feedCollectionInfo.Uri, feedCollectionInfo.MasterKey, feedCollectionInfo.ConnectionPolicy);
            await IntegrationTestsHelper.CreateDocumentCollectionAsync(feedClient, feedCollectionInfo.DatabaseName, new DocumentCollection { Id = feedCollectionInfo.CollectionName }, 400);

            using (var client = new DocumentClient(leaseCollectionInfo.Uri, leaseCollectionInfo.MasterKey, leaseCollectionInfo.ConnectionPolicy))
            {
                await IntegrationTestsHelper.CreateDocumentCollectionAsync(client, leaseCollectionInfo.DatabaseName, new DocumentCollection { Id = leaseCollectionInfo.CollectionName }, 400);
            }

            TaskCompletionSource<bool> documentReceived = new TaskCompletionSource<bool>();
            Task Process(IChangeFeedObserverContext changeFeedObserverContext, IReadOnlyList<Document> readOnlyList)
            {
                if (readOnlyList.Any(d => d.GetPropertyValue<string>("body") == sessionId))
                {
                    documentReceived.SetResult(true);
                }
                return Task.CompletedTask;
            }

            TaskCompletionSource<bool> partitionOpened = new TaskCompletionSource<bool>();
            FeedProcessing.IChangeFeedObserverFactory observerFactory = new DelegatingMemoryObserverFactory(async ctx =>
            {
                await Task.Yield();
                partitionOpened.SetResult(true);
            }, (ctx, reason) => Task.CompletedTask, Process);

            IChangeFeedProcessor changeFeedProcessor = await new ChangeFeedProcessorBuilder()
                .WithObserverFactory(observerFactory)
                .WithHostName("smoke_test")
                .WithProcessorOptions(new ChangeFeedProcessorOptions()
                {
                    MaxItemCount = 50
                })
                .WithFeedCollection(feedCollectionInfo)
                .WithLeaseCollection(leaseCollectionInfo)
                .BuildAsync();

            await changeFeedProcessor.StartAsync().ConfigureAwait(false);

            try
            {
                await partitionOpened.Task;
                var document = new Document();
                document.SetPropertyValue("body", sessionId);
                var collectionUri = UriFactory.CreateDocumentCollectionUri(feedCollectionInfo.DatabaseName, feedCollectionInfo.CollectionName);
                await feedClient.CreateDocumentAsync(collectionUri, document);

                await documentReceived.Task;
            }
            finally
            {
                await changeFeedProcessor.StopAsync();
            }
        }

        private class DelegatingMemoryObserverFactory : FeedProcessing.IChangeFeedObserverFactory, FeedProcessing.IChangeFeedObserver
        {

            private readonly Func<IChangeFeedObserverContext, Task> _opened;
            private readonly Func<IChangeFeedObserverContext, FeedProcessing.ChangeFeedObserverCloseReason, Task> _closed;
            private Func<IChangeFeedObserverContext, IReadOnlyList<Document>, Task> _process;

            public DelegatingMemoryObserverFactory(Func<IChangeFeedObserverContext, Task> opened,
                Func<IChangeFeedObserverContext, FeedProcessing.ChangeFeedObserverCloseReason, Task> closed,
                Func<IChangeFeedObserverContext, IReadOnlyList<Document>, Task> process)
            {
                _opened = opened;
                _closed = closed;
                _process = process;
            }



            Task FeedProcessing.IChangeFeedObserver.OpenAsync(IChangeFeedObserverContext context)
            {
                return _opened != null ? _opened(context) : Task.CompletedTask;
            }

            Task FeedProcessing.IChangeFeedObserver.CloseAsync(IChangeFeedObserverContext context, FeedProcessing.ChangeFeedObserverCloseReason reason)
            {
                return _closed != null ? _closed(context, reason) : Task.CompletedTask;
            }

            Task FeedProcessing.IChangeFeedObserver.ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs,
                CancellationToken cancellationToken)
            {
                return _process != null ? _process(context, docs) : Task.CompletedTask;
            }

            public FeedProcessing.IChangeFeedObserver CreateObserver()
            {
                return this;
            }
        }
    }
}
