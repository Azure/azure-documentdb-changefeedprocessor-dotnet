//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// The ILeaseManager defines a way to perform operations with <see cref="ILease"/>.
    /// </summary>
    public interface ILeaseManager
    {
        /// <summary>
        /// Checks whether the lease exists and creates it if it does not exist.
        /// </summary>
        /// <param name="partitionId">Partition to work on</param>
        /// <param name="continuationToken">Continuation token if it exists</param>
        Task<ILease> CreateLeaseIfNotExistAsync(string partitionId, string continuationToken);

        /// <summary>
        /// Delete the lease.
        /// </summary>
        /// <param name="lease">Lease to remove</param>
        Task DeleteAsync(ILease lease);

        /// <summary>
        /// Acquire ownership of the lease.
        /// </summary>
        /// <param name="lease">Lease to acquire</param>
        /// <returns>Updated acquired lease</returns>
        /// <exception cref="LeaseLostException">Thrown if other host acquired concurrently</exception>
        Task<ILease> AcquireAsync(ILease lease);

        /// <summary>
        /// Release ownership of the lease.
        /// </summary>
        /// <param name="lease">Lease to acquire</param>
        /// <exception cref="LeaseLostException">Thrown if other host acquired the lease or lease was deleted</exception>
        Task ReleaseAsync(ILease lease);

        /// <summary>
        /// Renew the lease. Leases are periodically renewed to prevent expiration.
        /// </summary>
        /// <param name="lease">Lease to renew</param>
        /// <returns>Updated renewed lease</returns>
        /// <exception cref="LeaseLostException">Thrown if other host acquired the lease or lease was deleted</exception>
        Task<ILease> RenewAsync(ILease lease);

        /// <summary>
        /// Replace properties from the specified lease.
        /// </summary>
        /// <param name="leaseToUpdatePropertiesFrom">Lease containing new properties</param>
        /// <returns>Updated lease</returns>
        /// <exception cref="LeaseLostException">Thrown if other host acquired the lease</exception>
        Task<ILease> UpdatePropertiesAsync(ILease leaseToUpdatePropertiesFrom);
    }
}
