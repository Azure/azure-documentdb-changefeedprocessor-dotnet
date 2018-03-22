//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Threading.Tasks;

    internal interface IDocumentServiceLeaseUpdater
    {
        Task<ILease> UpdateLeaseAsync(ILease cachedLease, Uri documentUri, Func<ILease, ILease> updateLease);
    }
}