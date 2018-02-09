//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;

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

        public Task OpenAsync(ChangeFeedObserverContext context)
        {
            return this.observer.OpenAsync(context);
        }

        public Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return this.observer.CloseAsync(context, reason);
        }

        public async Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs)
        {
            await this.observer.ProcessChangesAsync(context, docs).ConfigureAwait(false);
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