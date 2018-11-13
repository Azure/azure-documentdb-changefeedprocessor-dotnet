//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Used to create request options for non-partitioned lease collections.
    /// </summary>
    internal class SinglePartitionRequestOptionsFactory : IRequestOptionsFactory
    {
        public FeedOptions CreateFeedOptions() => null;

        public RequestOptions CreateRequestOptions(ILease lease) => null;
    }
}
