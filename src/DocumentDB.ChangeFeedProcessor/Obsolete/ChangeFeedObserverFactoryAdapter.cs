using Microsoft.Azure.Documents.ChangeFeedProcessor.Processing;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedObserverFactoryAdapter : Processing.IChangeFeedObserverFactory
    {
        private IChangeFeedObserverFactory factory;

        internal ChangeFeedObserverFactoryAdapter(IChangeFeedObserverFactory factory)
        {
            this.factory = factory;
        }

        public Processing.IChangeFeedObserver CreateObserver()
        {
            IChangeFeedObserver observer = this.factory.CreateObserver();
            return new ChangeFeedObserverAdapter(observer);
        }
    }
}