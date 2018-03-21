using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedObserverAdapter<T> : FeedProcessing.IChangeFeedObserver where T : IChangeFeedObserver, new()
    {
        private T observer;

        public ChangeFeedObserverAdapter()
        {
            observer = new T();
        }

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            return this.observer.OpenAsync(new ChangeFeedObserverContextAdapter(context));
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return this.observer.CloseAsync(new ChangeFeedObserverContextAdapter(context), reason);
        }

        public Task ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            return this.observer.ProcessChangesAsync(new ChangeFeedObserverContextAdapter(context), docs);
        }
    }

    internal class ChangeFeedObserverAdapter : FeedProcessing.IChangeFeedObserver
    {
        private IChangeFeedObserver observer;

        internal ChangeFeedObserverAdapter(IChangeFeedObserver observer)
        {
            this.observer = observer;
        }

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            return this.observer.OpenAsync(new ChangeFeedObserverContextAdapter(context));
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return this.observer.CloseAsync(new ChangeFeedObserverContextAdapter(context), reason);
        }

        public Task ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            return this.observer.ProcessChangesAsync(new ChangeFeedObserverContextAdapter(context), docs);
        }
    }

}