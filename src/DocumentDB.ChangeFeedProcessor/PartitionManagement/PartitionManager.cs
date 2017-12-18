//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal class PartitionManager : IPartitionManager
    {
        private readonly IBootstrapper bootstrapper;
        private readonly IPartitionController partitionController;
        private readonly IPartitionLoadBalancer partitionLoadBalancer;
        private readonly IRemainingWorkEstimator remainingWorkEstimator;

        public PartitionManager(IBootstrapper bootstrapper, IPartitionController partitionController, IPartitionLoadBalancer partitionLoadBalancer, IRemainingWorkEstimator remainingWorkEstimator)
        {
            this.bootstrapper = bootstrapper;
            this.partitionController = partitionController;
            this.partitionLoadBalancer = partitionLoadBalancer;
            this.remainingWorkEstimator = remainingWorkEstimator;
        }

        public async Task StartAsync()
        {
            await this.bootstrapper.InitializeAsync().ConfigureAwait(false);
            await this.partitionController.InitializeAsync().ConfigureAwait(false);
            this.partitionLoadBalancer.Start();
        }

        public async Task StopAsync()
        {
            await this.partitionLoadBalancer.StopAsync().ConfigureAwait(false);
            await this.partitionController.ShutdownAsync().ConfigureAwait(false);
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            return await this.remainingWorkEstimator.GetEstimatedRemainingWork().ConfigureAwait(false);
        }
    }
}