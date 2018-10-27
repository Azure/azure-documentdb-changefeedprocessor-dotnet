//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    internal class DocumentServiceLeaseUpdater : IDocumentServiceLeaseUpdater
    {
        private const int RetryCountOnConflict = 5;
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly IChangeFeedDocumentClient client;

        public DocumentServiceLeaseUpdater(IChangeFeedDocumentClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            this.client = client;
        }

        // Note: requestOptions are only used for read and not for update.
        public async Task<ILease> UpdateLeaseAsync(ILease cachedLease, Uri documentUri, RequestOptions requestOptions, Func<ILease, ILease> updateLease)
        {
            ILease lease = cachedLease;
            for (int retryCount = RetryCountOnConflict; retryCount >= 0; retryCount--)
            {
                lease = updateLease(lease);
                if (lease == null)
                {
                    return null;
                }

                lease.Timestamp = DateTime.UtcNow;
                Document leaseDocument = await this.TryReplaceLeaseAsync(lease, documentUri).ConfigureAwait(false);
                if (leaseDocument != null)
                {
                    return DocumentServiceLease.FromDocument(leaseDocument);
                }

                Logger.InfoFormat("Partition {0} lease update conflict. Reading the current version of lease.", lease.PartitionId);
                Document document;
                try
                {
                    IResourceResponse<Document> response = await this.client.ReadDocumentAsync(
                        documentUri, requestOptions).ConfigureAwait(false);
                    document = response.Resource;
                }
                catch (DocumentClientException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    Logger.InfoFormat("Partition {0} lease no longer exists", lease.PartitionId);
                    throw new LeaseLostException(lease);
                }

                DocumentServiceLease serverLease = DocumentServiceLease.FromDocument(document);
                Logger.InfoFormat(
                    "Partition {0} update failed because the lease with token '{1}' was updated by host '{2}' with token '{3}'. Will retry, {4} retry(s) left.",
                    lease.PartitionId,
                    lease.ConcurrencyToken,
                    serverLease.Owner,
                    serverLease.ConcurrencyToken,
                    retryCount);

                lease = serverLease;
            }

            throw new LeaseLostException(lease);
        }

        private async Task<Document> TryReplaceLeaseAsync(ILease lease, Uri leaseUri)
        {
            try
            {
                IResourceResponse<Document> response = await this.client.ReplaceDocumentAsync(leaseUri, lease, this.CreateIfMatchOptions(lease)).ConfigureAwait(false);
                return response.Resource;
            }
            catch (DocumentClientException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                return null;
            }
            catch (DocumentClientException ex)
            {
                Logger.WarnFormat("Lease operation exception, status code: ", ex.StatusCode);
                if (ex.StatusCode == HttpStatusCode.Conflict ||
                    ex.StatusCode == HttpStatusCode.NotFound)
                {
                    throw new LeaseLostException(lease, ex, ex.StatusCode == HttpStatusCode.NotFound);
                }

                throw;
            }
        }

        private RequestOptions CreateIfMatchOptions(ILease lease)
        {
            var ifMatchCondition = new AccessCondition { Type = AccessConditionType.IfMatch, Condition = lease.ConcurrencyToken };
            return new RequestOptions { AccessCondition = ifMatchCondition };
        }
    }
}
