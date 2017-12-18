using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionSupervisor : IDisposable
    {
        Task RunAsync(CancellationToken shutdownToken);
    }
}