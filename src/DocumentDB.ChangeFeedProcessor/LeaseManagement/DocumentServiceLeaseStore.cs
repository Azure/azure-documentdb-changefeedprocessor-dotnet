//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
    using Microsoft.Azure.Documents.Client;

    internal class DocumentServiceLeaseStore : ILeaseStore
    {
        private readonly IChangeFeedDocumentClient client;
        private readonly DocumentCollectionInfo leaseStoreCollectionInfo;
        private readonly string containerNamePrefix;
        private readonly string leaseCollectionLink;
        private readonly IRequestOptionsFactory requestOptionsFactory;
        private string lockETag;

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

        public async Task<bool> AcquireInitializationLockAsync(TimeSpan lockTime)
        {
            string lockId = this.GetStoreLockName();
            var containerDocument = new Document { Id = lockId, TimeToLive = (int)lockTime.TotalSeconds };
            var document = await this.client.TryCreateDocumentAsync(
                this.leaseCollectionLink,
                containerDocument).ConfigureAwait(false);

            if (document != null)
            {
                this.lockETag = document.ETag;
                return true;
            }

            return false;
        }

        public async Task<bool> ReleaseInitializationLockAsync()
        {
            string lockId = this.GetStoreLockName();
            Uri documentUri = UriFactory.CreateDocumentUri(this.leaseStoreCollectionInfo.DatabaseName, this.leaseStoreCollectionInfo.CollectionName, lockId);

            var requestOptions = this.requestOptionsFactory.CreateRequestOptions(
                DocumentServiceLease.FromDocument(new Document { Id = lockId }));
            requestOptions = requestOptions ?? new RequestOptions();
            requestOptions.AccessCondition = new AccessCondition { Type = AccessConditionType.IfMatch, Condition = this.lockETag };

            var document = await this.client.TryDeleteDocumentAsync(
                documentUri,
                requestOptions).ConfigureAwait(false);

            if (document != null)
            {
                this.lockETag = null;
                return true;
            }

            return false;
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