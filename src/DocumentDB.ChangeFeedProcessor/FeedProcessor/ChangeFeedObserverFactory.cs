namespace DocumentDB.ChangeFeedProcessor.FeedProcessor
{
    internal class ChangeFeedObserverFactory<T> : IChangeFeedObserverFactory where T : IChangeFeedObserver, new()
    {
        public IChangeFeedObserver CreateObserver()
        {
            return new T();
        }
    }
}