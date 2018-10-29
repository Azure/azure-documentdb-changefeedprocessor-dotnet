//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    /// <summary>
    /// Lease manager that is using Azure Document Service as lease storage.
    /// Documents in lease collection are organized as this:
    /// ChangeFeed.federation|database_rid|collection_rid.info            -- container
    /// ChangeFeed.federation|database_rid|collection_rid..partitionId1   -- each partition
    /// ChangeFeed.federation|database_rid|collection_rid..partitionId2
    ///                                         ...
    /// </summary>
    internal class DocumentServiceLeaseManager : ILeaseManager
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly string containerNamePrefix;
        private readonly DocumentCollectionInfo leaseCollectionInfo;
        private readonly IChangeFeedDocumentClient client;
        private readonly IDocumentServiceLeaseUpdater leaseUpdater;
        private readonly IRequestOptionsFactory requestOptionsFactory;
        private readonly string leaseCollectionLink;
        private readonly string hostName;

        public DocumentServiceLeaseManager(
            IChangeFeedDocumentClient client,
            IDocumentServiceLeaseUpdater leaseUpdater,
            DocumentCollectionInfo leaseCollectionInfo,
            IRequestOptionsFactory requestOptionsFactory,
            string containerNamePrefix,
            string leaseCollectionLink,
            string hostName)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (leaseUpdater == null) throw new ArgumentNullException(nameof(leaseUpdater));
            if (leaseCollectionInfo == null) throw new ArgumentNullException(nameof(leaseCollectionInfo));
            if (requestOptionsFactory == null) throw new ArgumentException(nameof(requestOptionsFactory));
            if (containerNamePrefix == null) throw new ArgumentNullException(nameof(containerNamePrefix));
            if (leaseCollectionLink == null) throw new ArgumentNullException(nameof(leaseCollectionLink));
            if (string.IsNullOrEmpty(hostName)) throw new ArgumentNullException(nameof(hostName));

            this.leaseCollectionInfo = leaseCollectionInfo;
            this.containerNamePrefix = containerNamePrefix;
            this.client = client;
            this.leaseUpdater = leaseUpdater;
            this.requestOptionsFactory = requestOptionsFactory;
            this.leaseCollectionLink = leaseCollectionLink;
            this.hostName = hostName;
        }

        public async Task<IReadOnlyList<ILease>> ListAllLeasesAsync()
        {
            return await this.ListDocumentsAsync(this.GetPartitionLeasePrefix()).ConfigureAwait(false);
        }

        public async Task<IEnumerable<ILease>> ListOwnedLeasesAsync()
        {
            var ownedLeases = new List<ILease>();
            foreach (ILease lease in await this.ListAllLeasesAsync().ConfigureAwait(false))
            {
                if (string.Compare(lease.Owner, this.hostName, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    ownedLeases.Add(lease);
                }
            }

            return ownedLeases;
        }

        public async Task<ILease> CreateLeaseIfNotExistAsync(string partitionId, string continuationToken)
        {
            if (partitionId == null)
                throw new ArgumentNullException(nameof(partitionId));

            string leaseDocId = this.GetDocumentId(partitionId);
            var documentServiceLease = new DocumentServiceLease
            {
                Id = leaseDocId,
                PartitionId = partitionId,
                ContinuationToken = continuationToken,
            };

            bool created = await this.client.TryCreateDocumentAsync(this.leaseCollectionLink, documentServiceLease).ConfigureAwait(false);
            if (created)
            {
                Logger.InfoFormat("Created lease for partition {0}.", partitionId);
                return documentServiceLease;
            }

            Logger.InfoFormat("Some other host created lease for {0}.", partitionId);
            return null;
        }

        public async Task<ILease> CheckpointAsync(ILease lease, string continuationToken)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            if (string.IsNullOrEmpty(continuationToken))
                throw new ArgumentException("continuationToken must be a non-empty string", nameof(continuationToken));

            return await this.leaseUpdater.UpdateLeaseAsync(
                lease,
                this.CreateDocumentUri(lease.Id),
                this.requestOptionsFactory.CreateRequestOptions(lease),
                serverLease =>
                {
                    if (serverLease.Owner != lease.Owner)
                    {
                        Logger.InfoFormat("Partition {0} lease was taken over by owner '{1}'", lease.PartitionId, serverLease.Owner);
                        throw new LeaseLostException(lease);
                    }
                    serverLease.ContinuationToken = continuationToken;
                    return serverLease;
                }).ConfigureAwait(false);
        }

        public async Task<ILease> AcquireAsync(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            string oldOwner = lease.Owner;

            return await this.leaseUpdater.UpdateLeaseAsync(
                lease,
                this.CreateDocumentUri(lease.Id),
                this.requestOptionsFactory.CreateRequestOptions(lease),
                serverLease =>
                {
                    if (serverLease.Owner != oldOwner)
                    {
                        Logger.InfoFormat("Partition {0} lease was taken over by owner '{1}'", lease.PartitionId, serverLease.Owner);
                        throw new LeaseLostException(lease);
                    }
                    serverLease.Owner = this.hostName;
                    serverLease.Properties = lease.Properties;
                    return serverLease;
                }).ConfigureAwait(false);
        }

        public async Task<ILease> RenewAsync(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            // Get fresh lease. The assumption here is that checkpointing is done with higher frequency than lease renewal so almost
            // certainly the lease was updated in between.
            DocumentServiceLease refreshedLease = await this.TryGetLeaseAsync(lease).ConfigureAwait(false);
            if (refreshedLease == null)
            {
                Logger.InfoFormat("Partition {0} failed to renew lease. The lease is gone already.", lease.PartitionId);
                throw new LeaseLostException(lease);
            }

            return await this.leaseUpdater.UpdateLeaseAsync(
                refreshedLease,
                this.CreateDocumentUri(refreshedLease.Id),
                this.requestOptionsFactory.CreateRequestOptions(lease),
                serverLease =>
                {
                    if (serverLease.Owner != lease.Owner)
                    {
                        Logger.InfoFormat("Partition {0} lease was taken over by owner '{1}'", lease.PartitionId, serverLease.Owner);
                        throw new LeaseLostException(lease);
                    }
                    return serverLease;
                }).ConfigureAwait(false);
        }

        public async Task ReleaseAsync(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            DocumentServiceLease refreshedLease = await this.TryGetLeaseAsync(lease).ConfigureAwait(false);
            if (refreshedLease == null)
            {
                Logger.InfoFormat("Partition {0} failed to release lease. The lease is gone already.", lease.PartitionId);
                throw new LeaseLostException(lease);
            }

            await this.leaseUpdater.UpdateLeaseAsync(
                refreshedLease,
                this.CreateDocumentUri(refreshedLease.Id),
                this.requestOptionsFactory.CreateRequestOptions(lease),
                serverLease =>
                {
                    if (serverLease.Owner != lease.Owner)
                    {
                        Logger.InfoFormat("Partition {0} no need to release lease. The lease was already taken by another host '{1}.", lease.PartitionId, serverLease.Owner);
                        throw new LeaseLostException(lease);
                    }
                    serverLease.Owner = null;
                    return serverLease;
                }).ConfigureAwait(false);
        }

        public async Task DeleteAsync(ILease lease)
        {
            if (lease?.Id == null)
                throw new ArgumentNullException(nameof(lease));

            Uri leaseUri = this.CreateDocumentUri(lease.Id);
            try
            {
                await this.parameters.Client.DeleteDocumentAsync(
                    leaseUri,
                    this.requestOptionsFactory.CreateRequestOptions(lease)).ConfigureAwait(false);
            }
            catch (DocumentClientException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Ignore - document was already deleted
            }
        }

        public async Task<ILease> UpdatePropertiesAsync(ILease lease)
        {
            if (lease == null) throw new ArgumentNullException(nameof(lease));

            if (lease.Owner != this.hostName)
            {
                Logger.InfoFormat("Partition '{0}' lease was taken over by owner '{1}' before lease properties update", lease.PartitionId, lease.Owner);
                throw new LeaseLostException(lease);
            }

            return await this.leaseUpdater.UpdateLeaseAsync(
                lease,
                this.CreateDocumentUri(lease.Id),
                this.requestOptionsFactory.CreateRequestOptions(lease),
                serverLease =>
                    {
                        if (serverLease.Owner != lease.Owner)
                        {
                            Logger.InfoFormat("Partition '{0}' lease was taken over by owner '{1}'", lease.PartitionId, serverLease.Owner);
                            throw new LeaseLostException(lease);
                        }
                        serverLease.Properties = lease.Properties;
                        return serverLease;
                    }).ConfigureAwait(false);
        }

        private async Task<DocumentServiceLease> TryGetLeaseAsync(ILease lease)
        {
            Uri documentUri = this.CreateDocumentUri(lease.Id);
            Document document = await this.client.TryGetDocumentAsync(
                documentUri,
                this.requestOptionsFactory.CreateRequestOptions(lease))
                .ConfigureAwait(false);
            return document != null ? DocumentServiceLease.FromDocument(document) : null;
        }

        private async Task<IReadOnlyList<DocumentServiceLease>> ListDocumentsAsync(string prefix)
        {
            if (string.IsNullOrEmpty(prefix))
                throw new ArgumentException("Prefix must be non-empty string", nameof(prefix));

            var querySpec = new SqlQuerySpec(
                "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)",
                new SqlParameterCollection(new[] { new SqlParameter { Name = "@PartitionLeasePrefix", Value = prefix } }));
            IDocumentQuery<Document> query = this.client.CreateDocumentQuery<Document>(
                this.leaseCollectionLink,
                querySpec,
                this.requestOptionsFactory.CreateFeedOptions())
                .AsDocumentQuery();
            var leases = new List<DocumentServiceLease>();
            while (query.HasMoreResults)
            {
                leases.AddRange(await query.ExecuteNextAsync<DocumentServiceLease>().ConfigureAwait(false));
            }

            return leases;
        }

        private string GetDocumentId(string partitionId)
        {
            return this.GetPartitionLeasePrefix() + partitionId;
        }

        private string GetPartitionLeasePrefix()
        {
            return this.containerNamePrefix + "..";
        }

        private Uri CreateDocumentUri(string leaseId)
        {
            return UriFactory.CreateDocumentUri(this.leaseCollectionInfo.DatabaseName, this.leaseCollectionInfo.CollectionName, leaseId);
        }
    }
}