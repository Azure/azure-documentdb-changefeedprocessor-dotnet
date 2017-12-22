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
            await partitionManager.StartAsync().ConfigureAwait(false);
        }

        public async Task StopAsync()
        {
            await partitionManager.StopAsync().ConfigureAwait(false);
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            return await partitionManager.GetEstimatedRemainingWork().ConfigureAwait(false);
        }
    }
}