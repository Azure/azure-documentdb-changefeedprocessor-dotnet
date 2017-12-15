using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    internal interface IPartitionController
    {
        Task AddLeaseAsync(ILease lease);

        Task InitializeAsync();

        Task ShutdownAsync();
    }
}