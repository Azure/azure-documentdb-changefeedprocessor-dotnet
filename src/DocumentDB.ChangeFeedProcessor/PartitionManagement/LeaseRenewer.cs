using System;
using System.Threading;
using System.Threading.Tasks;
using DocumentDB.ChangeFeedProcessor.Exceptions;
using DocumentDB.ChangeFeedProcessor.Logging;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    internal class LeaseRenewer : ILeaseRenewer
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
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
                logger.Info("Renewer task started.");
                while (true)
                {
                    await RenewLeaseAsync(lease).ConfigureAwait(false);
                    await Task.Delay(leaseRenewInterval, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                logger.Info("Renewer task stopped.");
            }
            catch (Exception ex)
            {
                logger.FatalException("Renew lease loop failed", ex);
                throw;
            }
        }

        private async Task RenewLeaseAsync(ILease lease)
        {
            try
            {
                await leaseManager.RenewAsync(lease).ConfigureAwait(false);
            }
            catch (LeaseLostException leaseLostException)
            {
                logger.ErrorException("Lost lease on renew {0}.", leaseLostException, lease.PartitionId);
                throw;
            }
            catch (Exception ex)
            {
                logger.ErrorException("Failed to renew lease for partition {0}.", ex, lease.PartitionId);
            }
        }
    }
}