//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;

    /// <summary>
    /// Remaing estimated work on the partition
    /// </summary>
    public class RemainingPartitionWork
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RemainingPartitionWork"/> class.
        /// </summary>
        /// <param name="partitionKeyRangeId">The partition key range ID for which the remaining work is calculated</param>
        /// <param name="remainingWork">The ammount of documents remaining to be processed</param>
        public RemainingPartitionWork(string partitionKeyRangeId, long remainingWork)
        {
            if (string.IsNullOrEmpty(partitionKeyRangeId)) throw new ArgumentNullException(nameof(partitionKeyRangeId));

            this.PartitionKeyRangeId = partitionKeyRangeId;
            this.RemainingWork = remainingWork;
        }

        /// <summary>
        /// Gets the partition key range ID for which the remaining work is calculated
        /// </summary>
        public string PartitionKeyRangeId { get; }

        /// <summary>
        /// Gets the ammount of documents remaining to be processed.
        /// </summary>
        public long RemainingWork { get; }
    }
}