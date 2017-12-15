using System;
using System.Threading.Tasks;
using DocumentDB.ChangeFeedProcessor.Adapters;
using DocumentDB.ChangeFeedProcessor.Utils;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace DocumentDB.ChangeFeedProcessor.Bootstrapping
{
    class LeaseStore : ILeaseStore
    {
        private readonly IDocumentClientEx client;
        private readonly DocumentCollectionInfo leaseStoreCollectionInfo;
        private readonly string containerNamePrefix;
        private readonly string leaseStoreCollectionLink;

        public LeaseStore(IDocumentClientEx client, DocumentCollectionInfo leaseStoreCollectionInfo, string containerNamePrefix, string leaseStoreCollectionLink)
        {
            this.client = client;
            this.leaseStoreCollectionInfo = leaseStoreCollectionInfo;
            this.containerNamePrefix = containerNamePrefix;
            this.leaseStoreCollectionLink = leaseStoreCollectionLink;
        }

        public async Task<bool> IsInitializedAsync()
        {
            string markerDocId = GetStoreMarkerName();
            Uri documentUri = UriFactory.CreateDocumentUri(leaseStoreCollectionInfo.DatabaseName, leaseStoreCollectionInfo.CollectionName, markerDocId);
            Document document = await client.TryGetDocumentAsync(documentUri).ConfigureAwait(false);
            return document != null;
        }

        public async Task MarkInitializedAsync()
        {
            string markerDocId = GetStoreMarkerName();
            var containerDocument = new Document { Id = markerDocId };
            await client.TryCreateDocumentAsync(leaseStoreCollectionLink, containerDocument).ConfigureAwait(false);
        }

        public async Task<bool> LockInitializationAsync(TimeSpan lockTime)
        {
            string lockId = GetStoreLockName();
            var containerDocument = new Document { Id = lockId, TimeToLive = (int)lockTime.TotalSeconds };
            return await client.TryCreateDocumentAsync(leaseStoreCollectionLink, containerDocument).ConfigureAwait(false);
        }

        private string GetStoreMarkerName()
        {
            return containerNamePrefix + ".info";
        }

        private string GetStoreLockName()
        {
            return containerNamePrefix + ".lock";
        }
    }
}