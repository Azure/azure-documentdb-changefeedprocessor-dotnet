//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Used to create DocumentServiceLeaseManager for partitioned lease collections, when partition key is defined as /id.
    /// </summary>
    internal class PartitionedByIdCollectionLeaseManagerFactory : ILeaseManagerFactory
    {
        public ILeaseManager Create(LeaseManagerParameters parameters)
        {
            return new LeaseManager(parameters);
        }

        private class LeaseManager : DocumentServiceLeaseManager
        {
            internal LeaseManager(LeaseManagerParameters parameters)
                : base(parameters, new RequestOptionsFactory())
            {
            }

            internal class RequestOptionsFactory : IRequestOptionsFactory
            {
                public RequestOptions CreateRequestOptions(ILease lease) => new RequestOptions { PartitionKey = new PartitionKey(lease.Id) };

                public FeedOptions CreateFeedOptions() => new FeedOptions { EnableCrossPartitionQuery = true };
            }
        }
    }
}
