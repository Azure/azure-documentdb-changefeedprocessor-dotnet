using System.Threading;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.FeedProcessor
{
    public interface IPartitionProcessor
    {
        Task RunAsync(CancellationToken cancellationToken);
    }
}