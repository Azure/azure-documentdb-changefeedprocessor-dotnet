using System.Threading.Tasks;
using DocumentDB.ChangeFeedProcessor.FeedProcessor;
using DocumentDB.ChangeFeedProcessor.Adapters;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionManagerBuilder
    {
        Task<IPartitionManager> BuildPartitionManagerAsync(string hostName, string leasePrefix, IChangeFeedObserverFactory observerFactory,
            IDocumentClientEx feedDocumentClient, DocumentCollectionInfo feedCollectionInfo, ChangeFeedHostOptions options);
    }
}