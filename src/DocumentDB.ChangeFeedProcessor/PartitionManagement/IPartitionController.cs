//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal interface IPartitionController
    {
        Task AddLeaseAsync(ILease lease);

        Task InitializeAsync();

        Task ShutdownAsync();
    }
}