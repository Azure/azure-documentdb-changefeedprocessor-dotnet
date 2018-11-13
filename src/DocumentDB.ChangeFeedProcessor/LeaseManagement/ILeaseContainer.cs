//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// Represents operations to get leases from lease store.
    /// </summary>
    public interface ILeaseContainer
    {
        /// <summary>
        /// Get all leases.
        /// </summary>
        /// <returns>List of all leases</returns>
        Task<IReadOnlyList<ILease>> GetAllLeasesAsync();

        /// <summary>
        /// Get all the leases owned by the current host.
        /// </summary>
        /// <returns>Enumerable of all leases owned by the current host</returns>
        Task<IEnumerable<ILease>> GetOwnedLeasesAsync();
    }
}
