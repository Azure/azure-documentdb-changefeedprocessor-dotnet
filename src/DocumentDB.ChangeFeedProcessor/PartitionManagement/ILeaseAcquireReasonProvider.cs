// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;

    /// <summary>
    /// Provides extensibility for <see cref="ILease"/> interface.
    /// <see cref="ILease"/> implementation can also can implement this interface to utilize the ability to set lease acquire reason from <see cref="IParitionLoadBalancingStrategy"/>.
    /// Lease acquire reason allows <see cref="ILeaseStoreManager"/> to handle conflicts on lease acquisition differently for different reasons.
    /// </summary>
    /// <remarks>
    /// If lease was detected as <see cref="LeaseAcquireReason.Expired"/>, etag conflict with a previous owner will fail the lease acquisition. Newer lease version indicates that the decision about lease expiration should be re-evaluated.
    /// For other acquire reasons such conflicts with unchanged owner can be retried.
    /// </remarks>
    public interface ILeaseAcquireReasonProvider
    {
        /// <summary>
        /// Gets or sets a reason of acquiring a lease which can be managed from <see cref="IParitionLoadBalancingStrategy"/>.
        /// </summary>
        LeaseAcquireReason AcquireReason { get; set; }
    }
}
