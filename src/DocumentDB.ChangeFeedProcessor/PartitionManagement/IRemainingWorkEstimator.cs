//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Used to estimate the pending work remaining to be read in the Change Feed. Calculates the sum of pending work based on the difference between the latest status of the feed and the status of each existing lease.
    /// </summary>
    public interface IRemainingWorkEstimator
    {
        /// <summary>
        /// Calculates an estimate of the pending work remaining to be read in the Change Feed in amount of documents in the whole collection.
        /// </summary>
        /// <returns>An estimation of pending work in amount of documents.</returns>
        Task<long> GetEstimatedRemainingWork();

        /// <summary>
        /// Calculates an estimate of the pending work remaining to be read in the Change Feed in amount of documents per partition.
        /// </summary>
        /// <returns>An array of an estimation of pending work in amount of documents per partitions.</returns>
        Task<IReadOnlyList<RemainingPartitionWork>> GetEstimatedRemainingWorkPerPartitionAsync();
    }
}
