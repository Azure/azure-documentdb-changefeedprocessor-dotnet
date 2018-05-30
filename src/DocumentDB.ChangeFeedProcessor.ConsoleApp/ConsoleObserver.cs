using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;

namespace DocumentDB.ChangeFeedProcessor.ConsoleApp
{
    public class ConsoleObserver : IChangeFeedObserver
    {
        private static int _totalDocuments = 0;

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            Console.WriteLine("Open:" + context.PartitionKeyRangeId);
            return Task.CompletedTask;
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            Console.WriteLine("Closed:" + context.PartitionKeyRangeId);
            return Task.CompletedTask;
        }

        public async Task ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            int result = Interlocked.Add(ref _totalDocuments, docs.Count);
            Console.WriteLine(context.PartitionKeyRangeId + ":" + docs.LastOrDefault()?.ResourceId + ":" + result + ":" + docs.Count);
            await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
        }
    }
}