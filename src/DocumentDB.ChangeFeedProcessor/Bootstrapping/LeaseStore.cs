﻿//----------------------------------------------------------------
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

    internal class LeaseStore : ILeaseStore
    {
        private readonly IChangeFeedDocumentClient client;
        private readonly DocumentCollectionInfo leaseStoreCollectionInfo;
        private readonly string containerNamePrefix;
        private readonly CollectionMetadata leaseCollectionMetadata;

        public LeaseStore(
            IChangeFeedDocumentClient client,
            DocumentCollectionInfo leaseStoreCollectionInfo,
            string containerNamePrefix,
            CollectionMetadata leaseCollectionMetadata)
        {
            this.client = client;
            this.leaseStoreCollectionInfo = leaseStoreCollectionInfo;
            this.containerNamePrefix = containerNamePrefix;
            this.leaseCollectionMetadata = leaseCollectionMetadata;
        }

        public async Task<bool> IsInitializedAsync()
        {
            string markerDocId = this.GetStoreMarkerName();
            Uri documentUri = UriFactory.CreateDocumentUri(this.leaseStoreCollectionInfo.DatabaseName, this.leaseStoreCollectionInfo.CollectionName, markerDocId);
            var requestOptions = this.leaseCollectionMetadata.GetRequestOptionsWithPartitionKeyById(markerDocId);

            Document document = await this.client.TryGetDocumentAsync(documentUri, requestOptions).ConfigureAwait(false);
            return document != null;
        }

        public async Task MarkInitializedAsync()
        {
            string markerDocId = this.GetStoreMarkerName();
            var containerDocument = new Document { Id = markerDocId };
            await this.client.TryCreateDocumentAsync(this.leaseCollectionMetadata.SelfLink, containerDocument).ConfigureAwait(false);
        }

        public async Task<bool> LockInitializationAsync(TimeSpan lockTime)
        {
            string lockId = this.GetStoreLockName();
            var containerDocument = new Document { Id = lockId, TimeToLive = (int)lockTime.TotalSeconds };
            return await this.client.TryCreateDocumentAsync(this.leaseCollectionMetadata.SelfLink, containerDocument).ConfigureAwait(false);
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