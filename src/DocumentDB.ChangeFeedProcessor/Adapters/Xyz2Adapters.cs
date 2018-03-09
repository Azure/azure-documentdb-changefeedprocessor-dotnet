using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters
{
    internal class ChangeFeedObserverFactoryAdapter : IObserverFactory
    {
        private IChangeFeedObserverFactory factory;

        internal ChangeFeedObserverFactoryAdapter(IChangeFeedObserverFactory factory)
        {
            this.factory = factory;
        }

        public IObserver CreateObserver()
        {
            IChangeFeedObserver observer = this.factory.CreateObserver();
            return new ChangeFeedObserverAdapter(observer);
        }
    }

    internal class ChangeFeedObserverAdapter : IObserver
    {
        private IChangeFeedObserver observer;

        internal ChangeFeedObserverAdapter(IChangeFeedObserver observer)
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
