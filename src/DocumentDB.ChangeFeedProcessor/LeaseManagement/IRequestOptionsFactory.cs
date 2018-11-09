//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Defines request options for lease requests to use with <see cref="DocumentServiceLeaseStoreManager"/> and <see cref="DocumentServiceLeaseStore"/>.
    /// </summary>
    internal interface IRequestOptionsFactory
    {
        RequestOptions CreateRequestOptions(ILease lease);

        FeedOptions CreateFeedOptions();
    }
}
