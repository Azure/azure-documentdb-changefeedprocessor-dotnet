//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedHost : IChangeFeedProcessor
    {
        private readonly IChangeFeedProcessor partitionManager;

        public ChangeFeedHost(IChangeFeedProcessor partitionManager)
        {
            if (partitionManager == null) throw new ArgumentNullException(nameof(partitionManager));
            this.partitionManager = partitionManager;
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
            return await this.partitionManager.GetEstimatedRemainingWork().ConfigureAwait(false);
        }
    }
}