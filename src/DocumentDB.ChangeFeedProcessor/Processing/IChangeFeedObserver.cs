using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Processing
{
    public interface IChangeFeedObserver
    {
        Task OpenAsync(IChangeFeedObserverContext context);

        Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason);

        Task ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken);
    }
}