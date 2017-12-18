namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    /// <summary>
    /// Factory class used to create instance(s) of <see cref="IChangeFeedObserver"/>.
    /// </summary>
    public interface IChangeFeedObserverFactory
    {
        /// <summary>
        /// Creates a new instance of <see cref="IChangeFeedObserver"/>.
        /// </summary>
        /// <returns>Created instance of <see cref="IChangeFeedObserver"/>.</returns>
        IChangeFeedObserver CreateObserver();
    }
}