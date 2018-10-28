//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    internal interface IDocumentServiceLeaseUpdater
    {
        Task<ILease> UpdateLeaseAsync(ILease cachedLease, Uri documentUri, RequestOptions requestOptions, Func<ILease, ILease> updateLease);
    }
}