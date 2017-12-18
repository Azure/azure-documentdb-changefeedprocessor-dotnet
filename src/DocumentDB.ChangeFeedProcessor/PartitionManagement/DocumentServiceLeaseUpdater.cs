using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal class DocumentServiceLeaseUpdater : IDocumentServiceLeaseUpdater
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private const int RetryCountOnConflict = 5;
        private readonly IDocumentClientEx client;

        public DocumentServiceLeaseUpdater(IDocumentClientEx client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            this.client = client;
        }

        public async Task<ILease> UpdateLeaseAsync(ILease cachedLease, Uri documentUri, Func<ILease, ILease> updateLease)
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
                Document leaseDocument = await TryReplaceLeaseAsync(lease, documentUri).ConfigureAwait(false);
                if (leaseDocument != null)
                {
                    return DocumentServiceLease.FromDocument(leaseDocument);
                }

                logger.InfoFormat("Partition '{0}' lease update conflict. Reading the the current version of lease.", lease.PartitionId);
                Document document;
                try
                {
                    document = await client.ReadDocumentAsync(documentUri).ConfigureAwait(false);
                }
                catch (DocumentClientException ex) when(HttpStatusCode.NotFound == ex.StatusCode)
                {
                    logger.InfoFormat("Partition '{0}' lease no longer exists", lease.PartitionId);
                    throw new LeaseLostException(lease);
                }

                DocumentServiceLease serverLease = DocumentServiceLease.FromDocument(document);
                logger.InfoFormat("Partition '{0}' update failed because the lease with token '{1}' was updated by host '{2}' with token '{3}'. Will retry, {4} retry(s) left.",
                    lease.PartitionId, lease.ConcurrencyToken, serverLease.Owner, serverLease.ConcurrencyToken, retryCount);

                lease = serverLease;
            }
            throw new LeaseLostException(lease);
        }

        private async Task<Document> TryReplaceLeaseAsync(ILease lease, Uri leaseUri)
        {
            try
            {
                return await client.ReplaceDocumentAsync(leaseUri, lease, CreateIfMatchOptions(lease)).ConfigureAwait(false);
            }
            catch (DocumentClientException ex) when (HttpStatusCode.PreconditionFailed == ex.StatusCode)
            {
                return null;
            }
            catch (DocumentClientException ex)
            {
                logger.WarnFormat("Lease operation exception, status code: ", ex.StatusCode);
                if (HttpStatusCode.Conflict == ex.StatusCode ||
                    HttpStatusCode.NotFound == ex.StatusCode)
                {
                    throw new LeaseLostException(lease, ex, HttpStatusCode.NotFound == ex.StatusCode);
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
