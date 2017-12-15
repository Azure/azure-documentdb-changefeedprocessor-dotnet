using System;
using System.Threading.Tasks;
using DocumentDB.ChangeFeedProcessor.PartitionManagement;

namespace DocumentDB.ChangeFeedProcessor
{
    internal class ChangeFeedHost : IChangeFeedHost
    {
        private readonly IPartitionManager partitionManager;

        public ChangeFeedHost(IPartitionManager partitionManager)
        {
            if (partitionManager == null) throw new ArgumentNullException(nameof(partitionManager));
            this.partitionManager = partitionManager;
        }

        public async Task StartAsync()
        {
            await partitionManager.StartAsync().ConfigureAwait(false);
        }

        public async Task StopAsync()
        {
            await partitionManager.StopAsync().ConfigureAwait(false);
        }
    }
}