using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionManagerBuilder
    {
        Task<IPartitionManager> BuildPartitionManagerAsync(string hostName, string leasePrefix, IChangeFeedObserverFactory observerFactory,
            IDocumentClientEx feedDocumentClient, DocumentCollectionInfo feedCollectionInfo, ChangeFeedHostOptions options);
    }
}