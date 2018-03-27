//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading.Tasks;

    /// <summary>
    /// Estimates pending work to be read in the Change Feed.
    /// </summary>
    public interface IRemainingWorkEstimator
    {
        /// <summary>
        /// Calculates an estimate of the pending work remaining to read.
        /// </summary>
        /// <returns>Returns an estimation of pending work.</returns>
        Task<long> GetEstimatedRemainingWork();
    }
}
