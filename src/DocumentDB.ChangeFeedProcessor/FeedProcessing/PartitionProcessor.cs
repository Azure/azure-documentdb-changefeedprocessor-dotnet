//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DocDBErrors;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.Client;

    internal class PartitionProcessor : IPartitionProcessor
    {
        private static readonly int DefaultMaxItemCount = 100;
        private readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly IChangeFeedDocumentQuery<Document> query;
        private readonly ProcessorSettings settings;
        private readonly IPartitionCheckpointer checkpointer;
        private readonly IChangeFeedObserver observer;
        private readonly ChangeFeedOptions options;

        public PartitionProcessor(IChangeFeedObserver observer, IChangeFeedDocumentClient documentClient, ProcessorSettings settings, IPartitionCheckpointer checkpointer)
        {
            this.observer = observer;
            this.settings = settings;
            this.checkpointer = checkpointer;
            this.options = new ChangeFeedOptions
            {
                MaxItemCount = settings.MaxItemCount,
                PartitionKeyRangeId = settings.PartitionKeyRangeId,
                SessionToken = settings.SessionToken,
                StartFromBeginning = settings.StartFromBeginning,
                RequestContinuation = settings.StartContinuation,
                StartTime = settings.StartTime,
            };

            this.query = documentClient.CreateDocumentChangeFeedQuery(settings.CollectionSelfLink, this.options);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            string lastContinuation = this.settings.StartContinuation;

            while (!cancellationToken.IsCancellationRequested)
            {
                TimeSpan delay = this.settings.FeedPollDelay;

                try
                {
                    do
                    {
                        IFeedResponse<Document> response = await this.query.ExecuteNextAsync<Document>(cancellationToken).ConfigureAwait(false);
                        lastContinuation = response.ResponseContinuation;
                        if (response.Count > 0)
                        {
                            await this.DispatchChanges(response, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    while (this.query.HasMoreResults && !cancellationToken.IsCancellationRequested);

                    if (this.options.MaxItemCount != this.settings.MaxItemCount)
                    {
                        this.options.MaxItemCount = this.settings.MaxItemCount;   // Reset after successful execution.
                    }
                }
                catch (DocumentClientException clientException)
                {
                    this.logger.WarnException("exception: partition '{0}'", clientException, this.settings.PartitionKeyRangeId);
                    DocDbError docDbError = ExceptionClassifier.ClassifyClientException(clientException);
                    switch (docDbError)
                    {
                        case DocDbError.PartitionNotFound:
                            throw new PartitionNotFoundException("Partition not found.", lastContinuation);
                        case DocDbError.PartitionSplit:
                            throw new PartitionSplitException("Partition split.", lastContinuation);
                        case DocDbError.Undefined:
                            throw;
                        case DocDbError.TransientError:
                            // Retry on transient (429) errors
                            break;
                        case DocDbError.MaxItemCountTooLarge:
                            if (!this.options.MaxItemCount.HasValue)
                            {
                                this.options.MaxItemCount = DefaultMaxItemCount;
                            }
                            else if (this.options.MaxItemCount <= 1)
                            {
                                this.logger.ErrorFormat("Cannot reduce maxItemCount further as it's already at {0}.", this.options.MaxItemCount);
                                throw;
                            }

                            this.options.MaxItemCount /= 2;
                            this.logger.WarnFormat("Reducing maxItemCount, new value: {0}.", this.options.MaxItemCount);
                            break;
                        default:
                            this.logger.Fatal($"Unrecognized DocDbError enum value {docDbError}");
                            Debug.Fail($"Unrecognized DocDbError enum value {docDbError}");
                            throw;
                    }

                    if (clientException.RetryAfter != TimeSpan.Zero)
                        delay = clientException.RetryAfter;
                }
                catch (TaskCanceledException canceledException)
                {
                    if (cancellationToken.IsCancellationRequested)
                        throw;

                    this.logger.WarnException("exception: partition '{0}'", canceledException, this.settings.PartitionKeyRangeId);

                    // ignore as it is caused by DocumentDB client
                }

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        private Task DispatchChanges(IFeedResponse<Document> response, CancellationToken cancellationToken)
        {
            IChangeFeedObserverContext context = new ChangeFeedObserverContext(this.settings.PartitionKeyRangeId, response, this.checkpointer);
            var docs = new List<Document>(response.Count);
            using (IEnumerator<Document> e = response.GetEnumerator())
            {
                while (e.MoveNext())
                {
                    docs.Add(e.Current);
                }
            }

            return this.observer.ProcessChangesAsync(context, docs, cancellationToken);
        }
    }
}