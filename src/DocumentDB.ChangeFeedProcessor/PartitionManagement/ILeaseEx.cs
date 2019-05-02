// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    /// <summary>
    /// Provides extensibility for <see cref="ILease"/> interface.
    /// </summary>
    public interface ILeaseEx : ILease
    {
        /// <summary>
        /// Gets or sets a reason of acquiring this lease which can be managed from <see cref="IParitionLoadBalancingStrategy"/>.
        /// </summary>
        AcquireReason? AcquireReason { get; set; }
    }
}
