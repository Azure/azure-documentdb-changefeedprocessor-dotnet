//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedEstimateHost : IRemainingWorkEstimator
    {
        private readonly IRemainingWorkEstimator remainingWorkEstimator;

        public ChangeFeedEstimateHost(IRemainingWorkEstimator remainingWorkEstimator)
        {
            if (remainingWorkEstimator == null) throw new ArgumentNullException(nameof(remainingWorkEstimator));

            this.remainingWorkEstimator = remainingWorkEstimator;
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            return await this.remainingWorkEstimator.GetEstimatedRemainingWork().ConfigureAwait(false);
        }
    }
}