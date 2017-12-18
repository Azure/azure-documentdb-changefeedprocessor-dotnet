//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    /// <summary>
    /// Read DocDB partitions and create leases if they do not exist
    /// </summary>
    public interface IPartitionSynchronizer
    {
        Task CreateMissingLeasesAsync();
        Task<IEnumerable<ILease>> SplitPartitionAsync(ILease lease);
    }
}