namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    public interface IChangeFeedObserverFactory
    {
        IChangeFeedObserver CreateObserver();
    }
}