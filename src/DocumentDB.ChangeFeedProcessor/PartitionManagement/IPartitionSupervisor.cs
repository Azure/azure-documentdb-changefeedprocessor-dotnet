using System;
using System.Threading;
using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionSupervisor : IDisposable
    {
        Task RunAsync(CancellationToken shutdownToken);
    }
}