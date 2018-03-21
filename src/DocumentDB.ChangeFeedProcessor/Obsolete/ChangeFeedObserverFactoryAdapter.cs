namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedObserverFactoryAdapter : FeedProcessing.IChangeFeedObserverFactory
    {
        private IChangeFeedObserverFactory factory;

        internal ChangeFeedObserverFactoryAdapter(IChangeFeedObserverFactory factory)
        {
            this.factory = factory;
        }

        public FeedProcessing.IChangeFeedObserver CreateObserver()
        {
            IChangeFeedObserver observer = this.factory.CreateObserver();
            return new ChangeFeedObserverAdapter(observer);
        }
    }
}