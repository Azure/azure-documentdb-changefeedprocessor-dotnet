//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using Microsoft.Azure.Documents.Client;

    internal interface IRequestOptionsFactory
    {
        RequestOptions CreateRequestOptions(ILease lease);

        FeedOptions CreateFeedOptions();
    }
}
