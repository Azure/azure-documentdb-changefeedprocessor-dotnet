using System.Threading.Tasks;
using DocumentDB.ChangeFeedProcessor.PartitionManagement;

namespace DocumentDB.ChangeFeedProcessor
{
    public class PartitionCheckpointer : IPartitionCheckpointer
    {
        private readonly ILeaseManager leaseManager;
        private readonly ILease lease;

        public PartitionCheckpointer(ILeaseManager leaseManager, ILease lease)
        {
            this.leaseManager = leaseManager;
            this.lease = lease;
        }

        public async Task CheckpointPartitionAsync(string сontinuationToken)
        {
            await leaseManager.CheckpointAsync(lease, сontinuationToken).ConfigureAwait(false);
        }
    }
}