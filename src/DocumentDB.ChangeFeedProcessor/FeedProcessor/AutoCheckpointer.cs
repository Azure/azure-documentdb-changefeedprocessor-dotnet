﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
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
            return observer.OpenAsync(context);
        }

        public Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return observer.CloseAsync(context, reason);
        }

        public async Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs)
        {
            await observer.ProcessChangesAsync(context, docs).ConfigureAwait(false);
            processedDocCount += docs.Count;

            if (IsCheckpointNeeded())
            {
                await context.CheckpointAsync().ConfigureAwait(false);
                processedDocCount = 0;
                lastCheckpointTime = DateTime.UtcNow;
            }
        }

        private bool IsCheckpointNeeded()
        {
            if(!checkpointFrequency.ProcessedDocumentCount.HasValue && !checkpointFrequency.TimeInterval.HasValue)
            {
                return true;
            }

            if (processedDocCount >= checkpointFrequency.ProcessedDocumentCount)
                return true;

            TimeSpan delta = DateTime.UtcNow - lastCheckpointTime;
            if (delta >= checkpointFrequency.TimeInterval)
                return true;

            return false;
        }
    }
}