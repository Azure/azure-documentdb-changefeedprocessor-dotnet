//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using Microsoft.Azure.Documents.Client;

    internal class RequestOptionsFactoryForParitionedByIdColleciton : IRequestOptionsFactory
    {
        public RequestOptions CreateRequestOptions(ILease lease)
        {
            return new RequestOptions { PartitionKey = new PartitionKey(lease.Id) };
        }

        public FeedOptions CreateFeedOptions()
        {
            return new FeedOptions { EnableCrossPartitionQuery = true };
        }
    }
}
