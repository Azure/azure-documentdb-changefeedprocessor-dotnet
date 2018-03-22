
using System;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Obsolete.Adapters
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    internal class ChangeFeedObserverAdapter<T> : FeedProcessing.IChangeFeedObserver where T : IChangeFeedObserver, new()
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

        public Task CloseAsync(FeedProcessing.IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return this.observer.CloseAsync(new ChangeFeedObserverContextAdapter(context), reason);
        }

        public Task ProcessChangesAsync(FeedProcessing.IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            return this.observer.ProcessChangesAsync(new ChangeFeedObserverContextAdapter(context), docs);
        }
    }

    internal class ChangeFeedObserverAdapter : FeedProcessing.IChangeFeedObserver
    {
        private IChangeFeedObserver observer;

        internal ChangeFeedObserverAdapter(IChangeFeedObserver observer)
        {
            this.observer = observer ?? throw new ArgumentNullException(nameof(observer));
        }

        public Task OpenAsync(FeedProcessing.IChangeFeedObserverContext context)
        {
            return this.observer.OpenAsync(new ChangeFeedObserverContextAdapter(context));
        }

        public Task CloseAsync(FeedProcessing.IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return this.observer.CloseAsync(new ChangeFeedObserverContextAdapter(context), reason);
        }

        public Task ProcessChangesAsync(FeedProcessing.IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            return this.observer.ProcessChangesAsync(new ChangeFeedObserverContextAdapter(context), docs);
        }
    }

}