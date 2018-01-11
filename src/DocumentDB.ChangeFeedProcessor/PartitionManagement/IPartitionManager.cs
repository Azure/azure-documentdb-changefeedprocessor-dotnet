using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal interface IPartitionManager
    {
        Task StartAsync();
        Task StopAsync();
    }
}