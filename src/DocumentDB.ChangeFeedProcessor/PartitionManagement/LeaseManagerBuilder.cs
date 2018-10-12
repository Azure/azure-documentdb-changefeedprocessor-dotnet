//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

    internal class LeaseManagerBuilder : ILeaseManagerBuilder
    {
        private DocumentCollectionInfo leaseCollectionLocation;
        private IChangeFeedDocumentClient leaseDocumentClient;
        private string leasePrefix;
        private CollectionMetadata leaseCollectionMetadata;
        private string hostName;

        public LeaseManagerBuilder WithLeaseCollection(DocumentCollectionInfo leaseCollectionLocation)
        {
            if (leaseCollectionLocation == null) throw new ArgumentNullException(nameof(leaseCollectionLocation));

            this.leaseCollectionLocation = leaseCollectionLocation.Canonicalize();
            return this;
        }

        public LeaseManagerBuilder WithLeaseDocumentClient(IChangeFeedDocumentClient leaseDocumentClient)
        {
            if (leaseDocumentClient == null) throw new ArgumentNullException(nameof(leaseDocumentClient));

            this.leaseDocumentClient = leaseDocumentClient;
            return this;
        }

        public LeaseManagerBuilder WithLeasePrefix(string leasePrefix)
        {
            if (leasePrefix == null) throw new ArgumentNullException(nameof(leasePrefix));

            this.leasePrefix = leasePrefix;
            return this;
        }

        public LeaseManagerBuilder WithLeaseCollectionMetadata(CollectionMetadata leaseCollectionMetadata)
        {
            if (leaseCollectionMetadata == null) throw new ArgumentNullException(nameof(leaseCollectionMetadata));

            this.leaseCollectionMetadata = leaseCollectionMetadata;
            return this;
        }

        public LeaseManagerBuilder WithHostName(string hostName)
        {
            if (hostName == null) throw new ArgumentNullException(nameof(hostName));

            this.hostName = hostName;
            return this;
        }

        public async Task<ILeaseManager> BuildAsync()
        {
            if (this.leaseCollectionLocation == null) throw new InvalidOperationException(nameof(this.leaseCollectionLocation) + " was not specified");

            this.leaseDocumentClient = this.leaseDocumentClient ?? this.leaseCollectionLocation.CreateDocumentClient();

            if (this.leaseCollectionMetadata == null)
            {
                DocumentCollection leaseCollection = await this.leaseDocumentClient.GetDocumentCollectionAsync(
                    this.leaseCollectionLocation).ConfigureAwait(false);
                this.leaseCollectionMetadata = await this.leaseDocumentClient.GetCollectionMetadataAsync(
                    this.leaseCollectionLocation, true).ConfigureAwait(false);
            }

            var updater = new DocumentServiceLeaseUpdater(this.leaseDocumentClient);
            var documentServiceLeaseManager = new DocumentServiceLeaseManager(
                this.leaseDocumentClient,
                updater,
                this.leaseCollectionLocation,
                this.leaseCollectionMetadata,
                this.leasePrefix,
                this.hostName);
            return documentServiceLeaseManager;
        }
    }
}
