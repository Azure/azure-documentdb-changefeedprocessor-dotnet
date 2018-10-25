//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

    internal class LeaseManagerBuilder : ILeaseManagerBuilder
    {
        private DocumentCollectionInfo leaseCollectionLocation;
        private IChangeFeedDocumentClient leaseDocumentClient;
        private IRequestOptionsFactory requestOptionsFactory;
        private string leasePrefix;
        private string leaseCollectionLink;
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

        public LeaseManagerBuilder WithLeaseCollectionLink(string leaseCollectionLink)
        {
            if (leaseCollectionLink == null) throw new ArgumentNullException(nameof(leaseCollectionLink));
            this.leaseCollectionLink = leaseCollectionLink;
            return this;
        }

        public LeaseManagerBuilder WithRequestOptionsFactory(IRequestOptionsFactory requestOptionsFactory)
        {
            if (requestOptionsFactory == null) throw new ArgumentNullException(nameof(requestOptionsFactory));
            this.requestOptionsFactory = requestOptionsFactory;
            return this;
        }

        public LeaseManagerBuilder WithHostName(string hostName)
        {
            if (hostName == null) throw new ArgumentNullException(nameof(hostName));

            this.hostName = hostName;
            return this;
        }

        public Task<ILeaseManager> BuildAsync()
        {
            if (this.leaseCollectionLocation == null) throw new InvalidOperationException(nameof(this.leaseCollectionLocation) + " was not specified");
            if (this.leaseCollectionLink == null) throw new InvalidOperationException(nameof(this.leaseCollectionLink) + " was not specified");
            if (this.requestOptionsFactory == null) throw new InvalidOperationException(nameof(this.requestOptionsFactory) + " was not specified");

            this.leaseDocumentClient = this.leaseDocumentClient ?? this.leaseCollectionLocation.CreateDocumentClient();

            var updater = new DocumentServiceLeaseUpdater(this.leaseDocumentClient);
            var documentServiceLeaseManager = new DocumentServiceLeaseManager(
                this.leaseDocumentClient,
                updater,
                this.leaseCollectionLocation,
                this.requestOptionsFactory,
                this.leasePrefix,
                this.leaseCollectionLink,
                this.hostName);
            return Task.FromResult<ILeaseManager>(documentServiceLeaseManager);
        }
    }
}
