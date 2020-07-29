// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    /// <summary>
    /// The reason why <see cref="IParitionLoadBalancingStrategy"/> chose to acquire a lease.
    /// </summary>
    public enum LeaseAcquireReason
    {
        /// <summary>
        /// Unknown reason to acquire.
        /// </summary>
        Unknown,

        /// <summary>
        /// Lease has no owner.
        /// </summary>
        NotOwned,

        /// <summary>
        /// Lease is detected as expired.
        /// </summary>
        Expired,

        /// <summary>
        /// Lease needs to be stolen for load balancing.
        /// </summary>
        Steal,
    }
}
