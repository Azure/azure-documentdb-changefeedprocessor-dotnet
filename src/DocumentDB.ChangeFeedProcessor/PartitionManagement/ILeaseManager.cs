//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;

#if PRIVATE_API
    public
#else
    internal
#endif
    interface ILeaseManager
    {
        /// <summary>
        /// Checks whether lease exists and creates if does not exist.
        /// </summary>
        /// <param name="partitionId">Partition to work on</param>
        /// <param name="continuationToken">Continuation token if it exists</param>
        Task<ILease> CreateLeaseIfNotExistAsync(string partitionId, string continuationToken);

        /// <summary>
        /// Get all the leases
        /// </summary>
        /// <returns>Enumerable of all the leases</returns>
        Task<IEnumerable<ILease>> ListLeasesAsync();

        /// <summary>
        /// Renew the lease
        /// </summary>
        /// <param name="lease">Lease to renew</param>
        /// <returns>Updated renewed lease</returns>
        /// <exception cref="LeaseLostException">Thrown if other host acquired the lease or lease was deleted</exception>
        Task<ILease> RenewAsync(ILease lease);

        /// <summary>
        /// Update the lease
        /// </summary>
        /// <param name="lease">Lease to renew</param>
        /// <param name="continuationToken">Continuation token</param>
        /// <returns>Updated renewed lease</returns>
        /// <exception cref="LeaseLostException">Thrown if other host acquired the lease or lease was deleted</exception>
        Task<ILease> CheckpointAsync(ILease lease, string continuationToken);

        /// <summary>
        /// Acquire lease.
        /// </summary>
        /// <param name="lease">Lease to acquire</param>
        /// <param name="owner">Owner</param>
        /// <returns>Updated acquired lease</returns>
        /// <exception cref="LeaseLostException">Thrown if other host acquired concurrently</exception>
        Task<ILease> AcquireAsync(ILease lease, string owner);

        /// <summary>
        /// Release lease.
        /// </summary>
        /// <param name="lease">Lease to acquire</param>
        /// <exception cref="LeaseLostException">Thrown if other host acquired the lease or lease was deleted</exception>
        Task ReleaseAsync(ILease lease);

        /// <summary>
        /// Delete lease.
        /// </summary>
        /// <param name="lease">Lease to remove</param>
        Task DeleteAsync(ILease lease);
    }
}