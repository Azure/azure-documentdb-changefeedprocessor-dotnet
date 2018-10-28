//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    internal interface ILeaseManagerFactory
    {
        ILeaseManager Create(LeaseManagerParameters parameters);
    }
}
