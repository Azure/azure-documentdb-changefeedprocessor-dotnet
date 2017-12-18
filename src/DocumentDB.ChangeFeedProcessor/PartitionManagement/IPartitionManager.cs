using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionManager
    {
        Task StartAsync();
        Task StopAsync();
    }
}