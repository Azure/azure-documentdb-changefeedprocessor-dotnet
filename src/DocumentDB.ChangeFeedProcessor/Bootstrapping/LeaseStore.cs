//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
    using Microsoft.Azure.Documents.Client;

#pragma warning disable SA1649 // File name should match first type name. Will remove in next commit. Keep for now to minimize churn.
    internal class DocumentServiceLeaseStore : ILeaseStore
#pragma warning restore SA1649 // File name should match first type name
    {
        private readonly IChangeFeedDocumentClient client;
        private readonly DocumentCollectionInfo leaseStoreCollectionInfo;
        private readonly string containerNamePrefix;
        private readonly string leaseCollectionLink;
        private readonly IRequestOptionsFactory requestOptionsFactory;

        public DocumentServiceLeaseStore(
            IChangeFeedDocumentClient client,
            DocumentCollectionInfo leaseCollectionInfo,
            string containerNamePrefix,
            string leaseCollectionLink,
            IRequestOptionsFactory requestOptionsFactory)
        {
            this.client = client;
            this.leaseStoreCollectionInfo = leaseCollectionInfo;
            this.containerNamePrefix = containerNamePrefix;
            this.leaseCollectionLink = leaseCollectionLink;
            this.requestOptionsFactory = requestOptionsFactory;
        }

        public async Task<bool> IsInitializedAsync()
        {
            string markerDocId = this.GetStoreMarkerName();
            Uri documentUri = UriFactory.CreateDocumentUri(this.leaseStoreCollectionInfo.DatabaseName, this.leaseStoreCollectionInfo.CollectionName, markerDocId);
            var requestOptions = this.requestOptionsFactory.CreateRequestOptions(
                DocumentServiceLease.FromDocument(new Document { Id = markerDocId }));

            Document document = await this.client.TryGetDocumentAsync(documentUri, requestOptions).ConfigureAwait(false);
            return document != null;
        }

        public async Task MarkInitializedAsync()
        {
            string markerDocId = this.GetStoreMarkerName();
            var containerDocument = new Document { Id = markerDocId };
            await this.client.TryCreateDocumentAsync(this.leaseCollectionLink, containerDocument).ConfigureAwait(false);
        }

        public async Task<bool> LockInitializationAsync(TimeSpan lockTime)
        {
            string lockId = this.GetStoreLockName();
            var containerDocument = new Document { Id = lockId, TimeToLive = (int)lockTime.TotalSeconds };
            return await this.client.TryCreateDocumentAsync(this.leaseCollectionLink, containerDocument).ConfigureAwait(false);
        }

        private string GetStoreMarkerName()
        {
            return this.containerNamePrefix + ".info";
        }

        private string GetStoreLockName()
        {
            return this.containerNamePrefix + ".lock";
        }
    }
}