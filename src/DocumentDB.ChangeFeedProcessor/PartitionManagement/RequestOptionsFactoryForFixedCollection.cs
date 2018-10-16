//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using Microsoft.Azure.Documents.Client;

    internal class RequestOptionsFactoryForFixedCollection : IRequestOptionsFactory
    {
        public RequestOptions CreateRequestOptions(ILease lease)
        {
            return null;
        }

        public FeedOptions CreateFeedOptions()
        {
            return null;
        }
    }
}
