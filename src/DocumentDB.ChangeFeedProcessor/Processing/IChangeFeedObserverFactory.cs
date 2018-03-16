namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Processing
{
    public interface IChangeFeedObserverFactory
    {
        IChangeFeedObserver CreateObserver();
    }
}