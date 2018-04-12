//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Collections.Generic;

    /// <summary>
    /// A strategy defines which leases should be taken by the current host in a certain moment.
    /// </summary>
    public interface IParitionLoadBalancingStrategy
    {
        /// <summary>
        /// Select leases that should be taken for processing.
        /// It can set new <see cref="ILease.Properties"/> for all returned leases if needed, including currently owned leases.
        /// This method will be called periodically with <see cref="ChangeFeedHostOptions.LeaseAcquireInterval"/>
        /// </summary>
        /// <param name="allLeases">All leases</param>
        /// <returns>Leases that should be taken for processing by this host</returns>
        IEnumerable<ILease> SelectLeasesToTake(IEnumerable<ILease> allLeases);
    }
}