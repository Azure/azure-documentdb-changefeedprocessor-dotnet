using System.Threading;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    public interface ILeaseRenewer
    {
        /// <summary>
        /// Starts the lease renewer
        /// </summary>
        Task RunAsync(CancellationToken cancellationToken);
    }
}