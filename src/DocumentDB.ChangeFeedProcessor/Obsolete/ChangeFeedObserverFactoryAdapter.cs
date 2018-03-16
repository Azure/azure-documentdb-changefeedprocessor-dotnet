using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedObserverFactoryAdapter : IObserverFactory
    {
        private IChangeFeedObserverFactoryObsolete factory;

        internal ChangeFeedObserverFactoryAdapter(IChangeFeedObserverFactoryObsolete factory)
        {
            this.factory = factory;
        }

        public IObserver CreateObserver()
        {
            IChangeFeedObserverObsolete observer = this.factory.CreateObserver();
            return new ChangeFeedObserverAdapter(observer);
        }
    }
}