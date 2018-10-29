//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Used to create DocumentServiceLeaseManager for not partitioned lease collections.
    /// </summary>
    internal class FixedCollectionLeaseManagerFactory : ILeaseManagerFactory
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

            private class RequestOptionsFactory : IRequestOptionsFactory
            {
                public FeedOptions CreateFeedOptions() => null;

                public RequestOptions CreateRequestOptions(ILease lease) => null;
            }
        }
    }
}
