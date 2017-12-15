using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionManager
    {
        Task StartAsync();
        Task StopAsync();
    }
}