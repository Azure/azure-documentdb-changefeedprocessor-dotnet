//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedEstimateHost : IChangeFeedHost
    {
        private readonly IRemainingWorkEstimator remainingWorkEstimator;

        public ChangeFeedEstimateHost(IRemainingWorkEstimator remainingWorkEstimator)
        {
            if (remainingWorkEstimator == null)
            {
                throw new ArgumentNullException(nameof(remainingWorkEstimator));
            }

            this.remainingWorkEstimator = remainingWorkEstimator;
        }

        public async Task StartAsync()
        {
            throw new NotImplementedException("This instance of IChangeFeedHost was created for work estimation only.");
        }

        public async Task StopAsync()
        {
            throw new NotImplementedException("This instance of IChangeFeedHost was created for work estimation only.");
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            return await this.remainingWorkEstimator.GetEstimatedRemainingWork().ConfigureAwait(false);
        }
    }
}