//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal interface ILeaseManagerFactory
    {
        ILeaseManager Create(LeaseManagerParameters parameters);
    }
}
