using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace DocumentDB.ChangeFeedProcessor.Adapters
{
    internal class DocumentClientEx : IDocumentClientEx
    {
        private readonly DocumentClient documentClient;

        public DocumentClientEx(DocumentClient documentClient)
        {
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            this.documentClient = documentClient;
        }

        public async Task<FeedResponse<PartitionKeyRange>> ReadPartitionKeyRangeFeedAsync(string partitionKeyRangesOrCollectionLink, FeedOptions options)
        {
            return await documentClient.ReadPartitionKeyRangeFeedAsync(partitionKeyRangesOrCollectionLink, options).ConfigureAwait(false);
        }

        public IDocumentQueryEx<Document> CreateDocumentChangeFeedQuery(string collectionLink, ChangeFeedOptions feedOptions)
        {
            IDocumentQuery<Document> query =  documentClient.CreateDocumentChangeFeedQuery(collectionLink, feedOptions);
            return new DocumentQueryEx<Document>(query);
        }

        public async Task<ResourceResponse<Database>> ReadDatabaseAsync(Uri databaseUri, RequestOptions options)
        {
            return await documentClient.ReadDatabaseAsync(databaseUri, options).ConfigureAwait(false);
        }

        public async Task<ResourceResponse<DocumentCollection>> ReadDocumentCollectionAsync(Uri documentCollectionUri, RequestOptions options)
        {
            return await documentClient.ReadDocumentCollectionAsync(documentCollectionUri, options).ConfigureAwait(false);
        }

        public async Task<ResourceResponse<Document>> CreateDocumentAsync(string documentsFeedOrDatabaseLink, object document)
        {
            return await documentClient.CreateDocumentAsync(documentsFeedOrDatabaseLink, document).ConfigureAwait(false);
        }

        public async Task<ResourceResponse<Document>> DeleteDocumentAsync(Uri documentUri)
        {
            return await documentClient.DeleteDocumentAsync(documentUri).ConfigureAwait(false);
        }

        public async Task<ResourceResponse<Document>> ReplaceDocumentAsync(Uri documentUri, object document, RequestOptions options)
        {
            return await documentClient.ReplaceDocumentAsync(documentUri, document, options).ConfigureAwait(false);
        }

        public async Task<ResourceResponse<Document>> ReadDocumentAsync(Uri documentUri)
        {
            return await documentClient.ReadDocumentAsync(documentUri).ConfigureAwait(false);
        }

        public IQueryable<T> CreateDocumentQuery<T>(string documentCollectionUri, SqlQuerySpec querySpec)
        {
            return documentClient.CreateDocumentQuery<T>(documentCollectionUri, querySpec);
        }
    }
}
