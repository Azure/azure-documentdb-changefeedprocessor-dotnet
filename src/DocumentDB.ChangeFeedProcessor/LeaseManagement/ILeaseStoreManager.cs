//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// The ILeaseManager defines a way to perform operations with <see cref="ILease"/>.
    /// </summary>
    public interface ILeaseStoreManager : ILeaseContainer, ILeaseManager, ILeaseCheckpointer, ILeaseStore
    {
    }
}
