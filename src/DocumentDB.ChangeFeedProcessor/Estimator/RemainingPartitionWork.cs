//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Estimator
{
    /// <summary>
    /// Remaing estimated work on the partition
    /// </summary>
    public class RemainingPartitionWork
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RemainingPartitionWork"/> class.
        /// </summary>
        /// <param name="partitionRangeId">The partition range ID for which the remaining work is calculated</param>
        /// <param name="remainingWork">The ammount of documents remaining to be processed</param>
        public RemainingPartitionWork(string partitionRangeId, long remainingWork)
        {
            this.PartitionRangeId = partitionRangeId;
            this.RemainingWork = remainingWork;
        }

        /// <summary>
        /// Gets the partition range ID for which the remaining work is calculated
        /// </summary>
        public string PartitionRangeId { get; }

        /// <summary>
        /// Gets the ammount of documents remaining to be processed.
        /// </summary>
        public long RemainingWork { get; }
    }
}