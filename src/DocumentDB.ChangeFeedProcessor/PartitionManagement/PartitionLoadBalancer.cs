﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;

    internal class PartitionLoadBalancer : IPartitionLoadBalancer
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
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
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        public void Start()
        {
            if (this.runTask != null)
            {
                throw new InvalidOperationException("Already started");
            }

            this.runTask = this.RunAsync();
        }

        public async Task StopAsync()
        {
            if (this.runTask == null)
            {
                throw new InvalidOperationException("Start has to be called before stop");
            }

            this.cancellationTokenSource.Cancel();
            await this.runTask.ConfigureAwait(false);
        }

        private async Task RunAsync()
        {
            try
            {
                while (true)
                {
                    try
                    {
                        IEnumerable<ILease> allLeases = await this.leaseManager.ListLeasesAsync().ConfigureAwait(false);
                        IEnumerable<ILease> leasesToTake = this.loadBalancingStrategy.SelectLeasesToTake(allLeases);

                        foreach (ILease lease in leasesToTake)
                        {
                            try
                            {
                                await this.partitionController.AddLeaseAsync(lease).ConfigureAwait(false);
                            }
                            catch (Exception e)
                            {
                                Logger.ErrorException("Partition load balancer lease add/update iteration failed", e);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.ErrorException("Partition load balancer iteration failed", e);
                    }

                    await Task.Delay(this.leaseAcquireInterval, this.cancellationTokenSource.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Info("Partition load balancer task stopped.");
            }
        }
    }
}