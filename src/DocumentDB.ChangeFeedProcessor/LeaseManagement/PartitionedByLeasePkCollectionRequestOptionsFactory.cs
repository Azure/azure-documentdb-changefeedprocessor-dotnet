//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Used to create request options for partitioned lease collections, when partition key is defined as /leasepk.
    /// </summary>
    internal class PartitionedByLeasePkCollectionRequestOptionsFactory : IRequestOptionsFactory
    {
        public RequestOptions CreateRequestOptions(ILease lease)
        {
            var documentServiceLease = lease as DocumentServiceLease;
            if (documentServiceLease == null)
                throw new ArgumentException("lease is not of type DocumentServiceLease");
            if (string.IsNullOrWhiteSpace(documentServiceLease.LeasePartitionKey))
                throw new ArgumentNullException(nameof(documentServiceLease.LeasePartitionKey));

            return new RequestOptions { PartitionKey = new PartitionKey(documentServiceLease.LeasePartitionKey) };
        }

        public FeedOptions CreateFeedOptions() => new FeedOptions { EnableCrossPartitionQuery = true };
    }
}
