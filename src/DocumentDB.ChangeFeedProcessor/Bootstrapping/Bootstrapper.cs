using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping
{
    internal class Bootstrapper : IBootstrapper
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly IPartitionSynchronizer synchronizer;
        private readonly ILeaseStore leaseStore;
        private readonly TimeSpan lockTime;
        private readonly TimeSpan sleepTime;

        public Bootstrapper(IPartitionSynchronizer synchronizer, ILeaseStore leaseStore, TimeSpan lockTime, TimeSpan sleepTime)
        {
            if (synchronizer == null) throw new ArgumentNullException(nameof(synchronizer));
            if (leaseStore == null) throw new ArgumentNullException(nameof(leaseStore));
            if (lockTime <= TimeSpan.Zero) throw new ArgumentException("should be positive", nameof(lockTime));
            if (sleepTime <= TimeSpan.Zero) throw new ArgumentException("should be positive", nameof(sleepTime));

            this.synchronizer = synchronizer;
            this.leaseStore = leaseStore;
            this.lockTime = lockTime;
            this.sleepTime = sleepTime;
        }

        public async Task InitializeAsync()
        {
            while (true)
            {
                bool initialized = await leaseStore.IsInitializedAsync().ConfigureAwait(false);
                if (initialized) break;

                bool shouldInitialize = await leaseStore.LockInitializationAsync(lockTime).ConfigureAwait(false);
                if (!shouldInitialize)
                {
                    logger.InfoFormat("Another instance is initializing the store");
                    await Task.Delay(sleepTime).ConfigureAwait(false);
                    continue;
                }

                logger.InfoFormat("Initializing the store");
                await synchronizer.CreateMissingLeasesAsync().ConfigureAwait(false);
                await leaseStore.MarkInitializedAsync().ConfigureAwait(false);
                break;
            }

            logger.InfoFormat("The store is initialized");
        }
    }
}