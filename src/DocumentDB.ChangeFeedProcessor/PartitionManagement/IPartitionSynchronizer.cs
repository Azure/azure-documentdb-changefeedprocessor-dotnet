//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

#if PRIVATE_API
    public
#else
    internal
#endif
    /// <summary>
    /// Read DocDB partitions and create leases if they do not exist
    /// </summary>
    interface IPartitionSynchronizer
    {
        Task CreateMissingLeasesAsync();

        Task<IEnumerable<ILease>> SplitPartitionAsync(ILease lease);
    }
}