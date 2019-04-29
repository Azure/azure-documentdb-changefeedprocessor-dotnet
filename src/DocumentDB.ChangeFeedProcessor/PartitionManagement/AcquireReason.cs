// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    /// <summary>
    /// The acquire reason.
    /// </summary>
    public enum AcquireReason
    {
        /// <summary>
        /// Partition has no owner.
        /// </summary>
        NoOwner,

        /// <summary>
        /// Partition is detected as expired.
        /// </summary>
        Expired,

        /// <summary>
        /// Partition needs to be stolen for load balancing.
        /// </summary>
        ForceSteal,

        /// <summary>
        /// Other reason to acquire.
        /// </summary>
        Other,
    }
}
