//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Used to create request options for partitioned lease collections, when partition key is defined as /id.
    /// </summary>
    internal class PartitionedByIdCollectionRequestOptionsFactory : IRequestOptionsFactory
    {
        public RequestOptions CreateRequestOptions(ILease lease) => new RequestOptions { PartitionKey = new PartitionKey(lease.Id) };

        public FeedOptions CreateFeedOptions() => new FeedOptions { EnableCrossPartitionQuery = true };

        public void AddPartitionKeyIfNeeded(ILease lease, string partitionKey)
        {
        }

        public void AddPartitionKeyIfNeeded(Document doc, string partitionKey)
        {
        }
    }
}
