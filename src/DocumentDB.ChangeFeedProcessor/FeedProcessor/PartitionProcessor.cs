//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

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
        private static readonly int DefaiultMaxItemCount = 100;
        private readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly IDocumentClientEx documentClient;
        private readonly ProcessorSettings settings;
        private readonly IPartitionCheckpointer checkpointer;
        private readonly IChangeFeedObserver observer;
        private IDocumentQueryEx<Document> query;
        private int? maxItemCount;

        public PartitionProcessor(IChangeFeedObserver observer, IDocumentClientEx documentClient, ProcessorSettings settings, IPartitionCheckpointer checkpointer)
        {
            this.documentClient = documentClient;
            this.observer = observer;
            this.settings = settings;
            this.checkpointer = checkpointer;
            this.maxItemCount = settings.MaxItemCount;
            this.query = CreateChangeFeedQuery();
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
                        case DocDbError.MaxItemCountTooLarge:
                            if (!maxItemCount.HasValue) maxItemCount = DefaiultMaxItemCount;
                            else if (maxItemCount <= 1)
                            {
                                logger.ErrorFormat("Cannot reduce maxItemCount further as it's already at {0}.", maxItemCount);
                                throw;
                            }

                            maxItemCount /= 2;
                            logger.WarnFormat("Reducing maxItemCount, new value: {0}.", maxItemCount);

                            query = CreateChangeFeedQuery();
                            break;
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

                if (maxItemCount != settings.MaxItemCount)
                {
                    maxItemCount = settings.MaxItemCount;
                    query = CreateChangeFeedQuery();   // Reset query to default after reducing max item count.
                }

                await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
            }
        }

        private IDocumentQueryEx<Document> CreateChangeFeedQuery()
        {
            var options = new ChangeFeedOptions
            {
                MaxItemCount = this.maxItemCount,
                PartitionKeyRangeId = settings.PartitionKeyRangeId,
                SessionToken = settings.SessionToken,
                StartFromBeginning = settings.StartFromBeginning,
                RequestContinuation = settings.RequestContinuation,
                StartTime = settings.StartTime
            };

            return this.documentClient.CreateDocumentChangeFeedQuery(settings.CollectionSelfLink, options);
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