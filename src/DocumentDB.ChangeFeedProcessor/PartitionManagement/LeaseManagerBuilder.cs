//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

    /// <summary>
    /// Provides flexible way to build lease manager constructor parameters.
    /// For the actual creation of lease manager instance, delegates to lease manager factory.
    /// </summary>
    internal class LeaseManagerBuilder
    {
        private LeaseManagerParameters parameters = new LeaseManagerParameters();
        private ILeaseManagerFactory leaseManagerFactory;

        public LeaseManagerBuilder WithLeaseCollection(DocumentCollectionInfo leaseCollectionLocation)
        {
            if (leaseCollectionLocation == null) throw new ArgumentNullException(nameof(leaseCollectionLocation));

            this.parameters.LeaseCollectionInfo = leaseCollectionLocation.Canonicalize();
            return this;
        }

        public LeaseManagerBuilder WithLeaseDocumentClient(IChangeFeedDocumentClient leaseDocumentClient)
        {
            if (leaseDocumentClient == null) throw new ArgumentNullException(nameof(leaseDocumentClient));

            this.parameters.Client = leaseDocumentClient;
            return this;
        }

        public LeaseManagerBuilder WithLeasePrefix(string leasePrefix)
        {
            if (leasePrefix == null) throw new ArgumentNullException(nameof(leasePrefix));

            this.parameters.ContainerNamePrefix = leasePrefix;
            return this;
        }

        public LeaseManagerBuilder WithLeaseCollectionLink(string leaseCollectionLink)
        {
            if (leaseCollectionLink == null) throw new ArgumentNullException(nameof(leaseCollectionLink));

            this.parameters.LeaseCollectionLink = leaseCollectionLink;
            return this;
        }

        public LeaseManagerBuilder WithLeaseManagerFactory(ILeaseManagerFactory leaseManagerFactory)
        {
            if (leaseManagerFactory == null) throw new ArgumentNullException(nameof(leaseManagerFactory));

            this.leaseManagerFactory = leaseManagerFactory;
            return this;
        }

        public LeaseManagerBuilder WithHostName(string hostName)
        {
            if (hostName == null) throw new ArgumentNullException(nameof(hostName));

            this.parameters.HostName = hostName;
            return this;
        }

        public Task<ILeaseManager> BuildAsync()
        {
            if (this.parameters.LeaseCollectionInfo == null)
                throw new InvalidOperationException(nameof(this.parameters.LeaseCollectionInfo) + " was not specified");
            if (this.parameters.LeaseCollectionLink == null)
                throw new InvalidOperationException(nameof(this.parameters.LeaseCollectionLink) + " was not specified");
            if (this.leaseManagerFactory == null) throw new InvalidOperationException(nameof(this.leaseManagerFactory) + " was not specified");

            this.parameters.Client = this.parameters.Client ?? this.parameters.LeaseCollectionInfo.CreateDocumentClient();
            this.parameters.LeaseUpdater = new DocumentServiceLeaseUpdater(this.parameters.Client);

            return Task.FromResult<ILeaseManager>(this.leaseManagerFactory.Create(this.parameters));
        }
    }
}
