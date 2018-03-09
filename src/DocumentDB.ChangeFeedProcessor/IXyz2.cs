namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IObserverFactory
    {
        IObserver CreateObserver();
    }

    public interface IObserver
    {
        Task OpenAsync(ChangeFeedObserverContext context);

        Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason);

        Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken);
    }
}
