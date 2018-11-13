//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;

    internal class LeaseRenewer : ILeaseRenewer
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly ILeaseManager leaseManager;
        private readonly TimeSpan leaseRenewInterval;
        private ILease lease;

        public LeaseRenewer(ILease lease, ILeaseManager leaseManager, TimeSpan leaseRenewInterval)
        {
            this.lease = lease;
            this.leaseManager = leaseManager;
            this.leaseRenewInterval = leaseRenewInterval;
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                Logger.InfoFormat("Partition {0}: renewer task started.", this.lease.PartitionId);
                await Task.Delay(TimeSpan.FromTicks(this.leaseRenewInterval.Ticks / 2), cancellationToken).ConfigureAwait(false);

                while (true)
                {
                    await this.RenewAsync().ConfigureAwait(false);
                    await Task.Delay(this.leaseRenewInterval, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Logger.InfoFormat("Partition {0}: renewer task stopped.", this.lease.PartitionId);
            }
            catch (Exception ex)
            {
                Logger.FatalException("Partition {0}: renew lease loop failed", ex, this.lease.PartitionId);
                throw;
            }
        }

        private async Task RenewAsync()
        {
            try
            {
                var renewedLease = await this.leaseManager.RenewAsync(this.lease).ConfigureAwait(false);
                if (renewedLease != null) this.lease = renewedLease;

                Logger.InfoFormat("Partition {0}: renewed lease with result {1}", this.lease.PartitionId, renewedLease != null);
            }
            catch (LeaseLostException leaseLostException)
            {
                Logger.ErrorException("Partition {0}: lost lease on renew.", leaseLostException, this.lease.PartitionId);
                throw;
            }
            catch (Exception ex)
            {
                Logger.ErrorException("Partition {0}: failed to renew lease.", ex, this.lease.PartitionId);
            }
        }
    }
}