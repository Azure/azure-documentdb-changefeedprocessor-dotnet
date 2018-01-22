//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedHost : IChangeFeedHost
    {
        private readonly IChangeFeedProcessor partitionManager;
        private readonly IRemainingWorkEstimator remainingWorkEstimator;

        public ChangeFeedHost(IChangeFeedProcessor partitionManager, IRemainingWorkEstimator remainingWorkEstimator)
        {
            if (partitionManager == null)
            {
                throw new ArgumentNullException(nameof(partitionManager));
            }

            if (remainingWorkEstimator == null)
            {
                throw new ArgumentNullException(nameof(remainingWorkEstimator));
            }

            this.partitionManager = partitionManager;
            this.remainingWorkEstimator = remainingWorkEstimator;
        }

        public async Task StartAsync()
        {
            await this.partitionManager.StartAsync().ConfigureAwait(false);
        }

        public async Task StopAsync()
        {
            await this.partitionManager.StopAsync().ConfigureAwait(false);
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            return await this.remainingWorkEstimator.GetEstimatedRemainingWork().ConfigureAwait(false);
        }
    }
}