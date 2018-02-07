//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;

    internal class LeaseRenewer : ILeaseRenewer
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly ILease lease;
        private readonly ILeaseManager leaseManager;
        private readonly TimeSpan leaseRenewInterval;

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
                Logger.Info("Renewer task started.");
                while (true)
                {
                    await this.RenewLeaseAsync(this.lease).ConfigureAwait(false);
                    await Task.Delay(this.leaseRenewInterval, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Logger.Info("Renewer task stopped.");
            }
            catch (Exception ex)
            {
                Logger.FatalException("Renew lease loop failed", ex);
                throw;
            }
        }

        private async Task RenewLeaseAsync(ILease lease)
        {
            try
            {
                await this.leaseManager.RenewAsync(lease).ConfigureAwait(false);
            }
            catch (LeaseLostException leaseLostException)
            {
                Logger.ErrorException("Lost lease on renew {0}.", leaseLostException, lease.PartitionId);
                throw;
            }
            catch (Exception ex)
            {
                Logger.ErrorException("Failed to renew lease for partition {0}.", ex, lease.PartitionId);
            }
        }
    }
}