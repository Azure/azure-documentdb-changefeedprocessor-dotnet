using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DocDBErrors;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    internal class PartitionProcessor : IPartitionProcessor
    {
        private readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly IDocumentQueryEx<Document> query;
        private readonly ProcessorSettings settings;
        private readonly IPartitionCheckpointer checkpointer;
        private readonly IChangeFeedObserver observer;

        public PartitionProcessor(IChangeFeedObserver observer, IDocumentClientEx documentClient, ProcessorSettings settings, IPartitionCheckpointer checkpointer)
        {
            this.observer = observer;
            this.settings = settings;
            this.checkpointer = checkpointer;

            var options = new ChangeFeedOptions
            {
                MaxItemCount = settings.MaxItemCount,
                PartitionKeyRangeId = settings.PartitionKeyRangeId,
                //SessionToken = this.changeFeedOptions.SessionToken, // TODO: handle the rest of the parameters
                //StartFromBeginning = this.changeFeedOptions.StartFromBeginning,
                RequestContinuation = settings.RequestContinuation
            };

            query = documentClient.CreateDocumentChangeFeedQuery(settings.CollectionSelfLink, options);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            string requestContinuation = settings.RequestContinuation;

            while (!cancellationToken.IsCancellationRequested)
            {
                TimeSpan retryDelay = settings.FeedPollDelay;

                try
                {
                    requestContinuation = await ProcessBatch(cancellationToken).ConfigureAwait(false);
                }
                catch (DocumentClientException clientException)
                {
                    logger.WarnException("exception: partition '{0}'", clientException, settings.PartitionKeyRangeId);
                    DocDbError docDbError = ExceptionClassifier.ClassifyClientException(clientException);
                    switch (docDbError)
                    {
                        case DocDbError.PartitionNotFound:
                            throw new PartitionNotFoundException(requestContinuation);
                        case DocDbError.PartitionSplit:
                            throw new PartitionSplitException(requestContinuation);
                        case DocDbError.Undefined:
                            throw;
                    }

                    if (clientException.RetryAfter != TimeSpan.Zero)
                        retryDelay = clientException.RetryAfter;
                }
                catch (TaskCanceledException canceledException)
                {
                    if (cancellationToken.IsCancellationRequested)
                        throw;

                    logger.WarnException("exception: partition '{0}'", canceledException, settings.PartitionKeyRangeId);
                    // ignore as it is caused by DocumentDB client
                }

                await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task<string> ProcessBatch(CancellationToken cancellation)
        {
            string lastContinuation;
            do
            {
                IFeedResponse<Document> response = await query.ExecuteNextAsync<Document>(cancellation).ConfigureAwait(false);
                lastContinuation = response.ResponseContinuation;
                if (response.Count > 0)
                    await DispatchChanges(response).ConfigureAwait(false);
            }
            while (query.HasMoreResults && !cancellation.IsCancellationRequested);
            return lastContinuation;
        }

        private Task DispatchChanges(IFeedResponse<Document> response)
        {
            var context = new ChangeFeedObserverContext(settings.PartitionKeyRangeId, response, checkpointer);
            var docs = new List<Document>(response.Count);
            using (IEnumerator<Document> e = response.GetEnumerator())
            {
                while (e.MoveNext())
                {
                    docs.Add(e.Current);
                }
            }

            return observer.ProcessChangesAsync(context, docs);
        }
    }
}