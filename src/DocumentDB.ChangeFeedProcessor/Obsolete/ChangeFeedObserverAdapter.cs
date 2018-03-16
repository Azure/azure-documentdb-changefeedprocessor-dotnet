using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedObserverAdapter<T> : IObserver where T : IChangeFeedObserverObsolete, new()
    {
        private T observer;

        public ChangeFeedObserverAdapter()
        {
            observer = new T();
        }

        public Task OpenAsync(ChangeFeedObserverContext context)
        {
            return this.observer.OpenAsync(context);
        }

        public Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return this.observer.CloseAsync(context, reason);
        }

        public Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            return this.observer.ProcessChangesAsync(context, docs);
        }
    }

    internal class ChangeFeedObserverAdapter : IObserver
    {
        private IChangeFeedObserverObsolete observer;

        internal ChangeFeedObserverAdapter(IChangeFeedObserverObsolete observer)
        {
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

        public Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            return this.observer.ProcessChangesAsync(context, docs);
        }
    }

}