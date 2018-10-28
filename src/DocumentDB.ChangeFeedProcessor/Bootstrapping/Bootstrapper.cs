//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class Bootstrapper : IBootstrapper
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
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
                bool initialized = await this.leaseStore.IsInitializedAsync().ConfigureAwait(false);
                if (initialized) break;

                bool isLockAcquired = await this.leaseStore.AcquireInitializationLockAsync(this.lockTime).ConfigureAwait(false);

                try
                {
                    if (!isLockAcquired)
                    {
                        Logger.InfoFormat("Another instance is initializing the store");
                        await Task.Delay(this.sleepTime).ConfigureAwait(false);
                        continue;
                    }

                    Logger.InfoFormat("Initializing the store");
                    await this.synchronizer.CreateMissingLeasesAsync().ConfigureAwait(false);
                    await this.leaseStore.MarkInitializedAsync().ConfigureAwait(false);
                }
                finally
                {
                    if (isLockAcquired)
                    {
                        await this.leaseStore.ReleaseInitializationLockAsync().ConfigureAwait(false);
                    }
                }

                break;
            }

            Logger.InfoFormat("The store is initialized");
        }
    }
}