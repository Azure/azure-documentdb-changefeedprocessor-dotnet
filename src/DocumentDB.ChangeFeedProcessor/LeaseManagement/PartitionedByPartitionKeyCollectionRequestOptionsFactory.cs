//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Used to create request options for partitioned lease collections, when partition key is defined as /partitionKey.
    /// </summary>
    internal class PartitionedByPartitionKeyCollectionRequestOptionsFactory : IRequestOptionsFactory
    {
        public RequestOptions CreateRequestOptions(ILease lease)
        {
            var documentServiceLease = lease as DocumentServiceLease;
            if (documentServiceLease == null)
                throw new ArgumentException("lease is not of type DocumentServiceLease");
            if (string.IsNullOrWhiteSpace(documentServiceLease.PartitionKey))
                throw new ArgumentNullException(nameof(documentServiceLease.PartitionKey));

            return new RequestOptions { PartitionKey = new PartitionKey(documentServiceLease.PartitionKey) };
        }

        public FeedOptions CreateFeedOptions() => new FeedOptions { EnableCrossPartitionQuery = true };

        public void AddPartitionKeyIfNeeded(ILease lease, string partitionKey)
        {
            var documentServiceLease = lease as DocumentServiceLease;
            if (documentServiceLease == null)
                throw new ArgumentException("lease is not of type DocumentServiceLease");
            if (string.IsNullOrWhiteSpace(partitionKey))
                throw new ArgumentNullException(nameof(partitionKey));

            documentServiceLease.PartitionKey = partitionKey;
        }

        public void AddPartitionKeyIfNeeded(Document doc, string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
                throw new ArgumentNullException(nameof(partitionKey));

            doc.SetPropertyValue(DocumentServiceLease.LeasePartitionKeyPropertyName, partitionKey);
        }
    }
}
