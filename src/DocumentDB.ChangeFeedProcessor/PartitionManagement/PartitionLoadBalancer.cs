//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal class PartitionLoadBalancer : IPartitionLoadBalancer
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly IPartitionController partitionController;
        private readonly ILeaseManager leaseManager;
        private readonly ILoadBalancingStrategy loadBalancingStrategy;
        private readonly TimeSpan leaseAcquireInterval;
        private readonly CancellationTokenSource cancellationTokenSource;
        private Task runTask;

        public PartitionLoadBalancer(IPartitionController partitionController, ILeaseManager leaseManager, ILoadBalancingStrategy loadBalancingStrategy, TimeSpan leaseAcquireInterval)
        {
            if (partitionController == null) throw new ArgumentNullException(nameof(partitionController));
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            if (loadBalancingStrategy == null) throw new ArgumentNullException(nameof(loadBalancingStrategy));

            this.partitionController = partitionController;
            this.leaseManager = leaseManager;
            this.loadBalancingStrategy = loadBalancingStrategy;
            this.leaseAcquireInterval = leaseAcquireInterval;
            cancellationTokenSource = new CancellationTokenSource();
        }

        public void Start()
        {
            if (runTask != null)
            {
                throw new InvalidOperationException("Already started");
            }
            runTask = RunAsync();
        }

        public async Task StopAsync()
        {
            if (runTask == null)
            {
                throw new InvalidOperationException("Start has to be called before stop");
            }
            cancellationTokenSource.Cancel();
            await runTask.ConfigureAwait(false);
        }

        private async Task RunAsync()
        {
            try
            {
                while (true)
                {
                    IEnumerable<ILease> allLeases = await leaseManager.ListLeasesAsync().ConfigureAwait(false);
                    IEnumerable<ILease> leasesToTake = loadBalancingStrategy.CalculateLeasesToTake(allLeases);

                    foreach (ILease lease in leasesToTake)
                    {
                        await partitionController.AddLeaseAsync(lease).ConfigureAwait(false);
                    }
                    await Task.Delay(leaseAcquireInterval, cancellationTokenSource.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                logger.Info("Partition load balancer task stopped.");
            }
            catch (Exception ex)
            {
                logger.FatalException("Partition load balancer loop failed", ex);
                throw;
            }
        }
    }
}