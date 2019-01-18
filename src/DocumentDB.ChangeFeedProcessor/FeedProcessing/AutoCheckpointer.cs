//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;

    internal class AutoCheckpointer : IChangeFeedObserver
    {
        private readonly CheckpointFrequency checkpointFrequency;
        private readonly IChangeFeedObserver observer;
        private int processedDocCount;
        private DateTime lastCheckpointTime = DateTime.UtcNow;

        public AutoCheckpointer(CheckpointFrequency checkpointFrequency, IChangeFeedObserver observer)
        {
            if (checkpointFrequency == null)
                throw new ArgumentNullException(nameof(checkpointFrequency));
            if (observer == null)
                throw new ArgumentNullException(nameof(observer));

            this.checkpointFrequency = checkpointFrequency;
            this.observer = observer;
        }

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            return this.observer.OpenAsync(context);
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return this.observer.CloseAsync(context, reason);
        }

        public async Task ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            try
            {
                await this.observer.ProcessChangesAsync(context, docs, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception userException)
            {
                throw new UserException(userException);
            }

            this.processedDocCount += docs.Count;

            if (this.IsCheckpointNeeded())
            {
                await context.CheckpointAsync().ConfigureAwait(false);
                this.processedDocCount = 0;
                this.lastCheckpointTime = DateTime.UtcNow;
            }
        }

        private bool IsCheckpointNeeded()
        {
            if (!this.checkpointFrequency.ProcessedDocumentCount.HasValue && !this.checkpointFrequency.TimeInterval.HasValue)
            {
                return true;
            }

            if (this.processedDocCount >= this.checkpointFrequency.ProcessedDocumentCount)
                return true;

            TimeSpan delta = DateTime.UtcNow - this.lastCheckpointTime;
            if (delta >= this.checkpointFrequency.TimeInterval)
                return true;

            return false;
        }
    }
}