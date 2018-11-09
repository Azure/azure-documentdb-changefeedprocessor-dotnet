//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// Used to checkpoitn leases.
    /// </summary>
    public interface ILeaseCheckpointer
    {
        /// <summary>
        /// Checkpoint the lease.
        /// </summary>
        /// <param name="lease">Lease to renew</param>
        /// <param name="continuationToken">Continuation token</param>
        /// <returns>Updated renewed lease</returns>
        /// <exception cref="LeaseLostException">Thrown if other host acquired the lease or lease was deleted</exception>
        Task<ILease> CheckpointAsync(ILease lease, string continuationToken);
    }
}
