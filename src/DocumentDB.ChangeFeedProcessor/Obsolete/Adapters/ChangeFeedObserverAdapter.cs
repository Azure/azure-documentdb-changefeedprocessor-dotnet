//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Obsolete.Adapters
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    internal class ChangeFeedObserverAdapter<T> : FeedProcessing.IChangeFeedObserver
#pragma warning disable CS0618 // Type or member is obsolete
        where T : IChangeFeedObserver, new()
#pragma warning restore CS0618 // Type or member is obsolete
    {
        private T observer;

        public ChangeFeedObserverAdapter()
        {
            this.observer = new T();
        }

        public Task OpenAsync(FeedProcessing.IChangeFeedObserverContext context)
        {
            return this.observer.OpenAsync(new ChangeFeedObserverContextAdapter(context));
        }

        public Task CloseAsync(FeedProcessing.IChangeFeedObserverContext context, FeedProcessing.ChangeFeedObserverCloseReason reason)
        {
#pragma warning disable CS0618 // Type or member is obsolete
            return this.observer.CloseAsync(new ChangeFeedObserverContextAdapter(context), (ChangeFeedObserverCloseReason)reason);
#pragma warning restore CS0618 // Type or member is obsolete
        }

        public Task ProcessChangesAsync(FeedProcessing.IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            return this.observer.ProcessChangesAsync(new ChangeFeedObserverContextAdapter(context), docs);
        }
    }

    internal class ChangeFeedObserverAdapter : FeedProcessing.IChangeFeedObserver
    {
#pragma warning disable CS0618 // Type or member is obsolete
        private IChangeFeedObserver observer;
#pragma warning restore CS0618 // Type or member is obsolete

#pragma warning disable CS0618 // Type or member is obsolete
        internal ChangeFeedObserverAdapter(IChangeFeedObserver observer)
#pragma warning restore CS0618 // Type or member is obsolete
        {
            this.observer = observer ?? throw new ArgumentNullException(nameof(observer));
        }

        public Task OpenAsync(FeedProcessing.IChangeFeedObserverContext context)
        {
            return this.observer.OpenAsync(new ChangeFeedObserverContextAdapter(context));
        }

        public Task CloseAsync(FeedProcessing.IChangeFeedObserverContext context, FeedProcessing.ChangeFeedObserverCloseReason reason)
        {
#pragma warning disable CS0618 // Type or member is obsolete
            return this.observer.CloseAsync(new ChangeFeedObserverContextAdapter(context), (ChangeFeedObserverCloseReason)reason);
#pragma warning restore CS0618 // Type or member is obsolete
        }

        public Task ProcessChangesAsync(FeedProcessing.IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            return this.observer.ProcessChangesAsync(new ChangeFeedObserverContextAdapter(context), docs);
        }
    }
}