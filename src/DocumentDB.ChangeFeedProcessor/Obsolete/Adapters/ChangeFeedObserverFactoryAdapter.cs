using System;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Obsolete.Adapters
{
    internal class ChangeFeedObserverFactoryAdapter : FeedProcessing.IChangeFeedObserverFactory
    {
        private IChangeFeedObserverFactory factory;

        internal ChangeFeedObserverFactoryAdapter(IChangeFeedObserverFactory factory)
        {
            this.factory = factory ?? throw new ArgumentNullException(nameof(factory));
        }

        public FeedProcessing.IChangeFeedObserver CreateObserver()
        {
            IChangeFeedObserver observer = this.factory.CreateObserver();
            return new ChangeFeedObserverAdapter(observer);
        }
    }
}