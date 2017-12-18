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

        public PartitionManager(IBootstrapper bootstrapper, IPartitionController partitionController, IPartitionLoadBalancer partitionLoadBalancer)
        {
            this.bootstrapper = bootstrapper;
            this.partitionController = partitionController;
            this.partitionLoadBalancer = partitionLoadBalancer;
        }

        public async Task StartAsync()
        {
            await bootstrapper.InitializeAsync().ConfigureAwait(false);
            await partitionController.InitializeAsync().ConfigureAwait(false);
            partitionLoadBalancer.Start();
        }

        public async Task StopAsync()
        {
            await partitionLoadBalancer.StopAsync().ConfigureAwait(false);
            await partitionController.ShutdownAsync().ConfigureAwait(false);
        }
    }
}