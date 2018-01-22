//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal class PartitionManager : IChangeFeedProcessor
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
            await this.bootstrapper.InitializeAsync().ConfigureAwait(false);
            await this.partitionController.InitializeAsync().ConfigureAwait(false);
            this.partitionLoadBalancer.Start();
        }

        public async Task StopAsync()
        {
            await this.partitionLoadBalancer.StopAsync().ConfigureAwait(false);
            await this.partitionController.ShutdownAsync().ConfigureAwait(false);
        }
    }
}