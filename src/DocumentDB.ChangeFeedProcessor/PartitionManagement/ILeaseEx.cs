// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    /// <summary>
    /// Provides extensibility for <see cref="ILease"/> interface.
    /// <see cref="ILease"/> implementation can also can implement this interface to utilize the ability to set lease acquire reason from <see cref="IParitionLoadBalancingStrategy"/>.
    /// </summary>
    public interface ILeaseEx
    {
        /// <summary>
        /// Gets or sets a reason of acquiring this lease which can be managed from <see cref="IParitionLoadBalancingStrategy"/>.
        /// </summary>
        LeaseAcquireReason? LeaseAcquireReason { get; set; }
    }
}
