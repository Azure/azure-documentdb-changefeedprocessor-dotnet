//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal interface IPartitionSupervisorFactory
    {
        IPartitionSupervisor Create(ILease lease);
    }
}