namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionSupervisorFactory
    {
        IPartitionSupervisor Create(ILease lease);
    }
}