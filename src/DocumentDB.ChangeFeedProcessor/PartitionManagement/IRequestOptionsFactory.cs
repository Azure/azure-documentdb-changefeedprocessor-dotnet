//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Defines request options for lease requests to use with <see cref="ILeaseManager"/>.
    /// </summary>
    internal interface IRequestOptionsFactory
    {
        RequestOptions CreateRequestOptions(ILease lease);

        FeedOptions CreateFeedOptions();
    }
}
