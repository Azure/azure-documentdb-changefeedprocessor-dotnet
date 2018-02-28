//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
#if PRIVATE_API
    public
#else
    internal
#endif
    interface IPartitionSupervisorFactory
    {
        IPartitionSupervisor Create(ILease lease);
    }
}