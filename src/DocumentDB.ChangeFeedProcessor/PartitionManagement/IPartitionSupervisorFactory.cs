namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionSupervisorFactory
    {
        IPartitionSupervisor Create(ILease lease);
    }
}