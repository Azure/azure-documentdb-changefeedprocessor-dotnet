//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    /// <summary>
    /// DocumentClient used for Change Feed interaction.
    /// </summary>
    public class ChangeFeedDocumentClient : IChangeFeedDocumentClient
    {
        private readonly DocumentClient documentClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="ChangeFeedDocumentClient"/> class.
        /// </summary>
        /// <param name="documentClient">Existing <see cref="DocumentClient"/>.</param>
        public ChangeFeedDocumentClient(DocumentClient documentClient)
        {
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            this.documentClient = documentClient;
        }

        /// <summary>
        /// Reads the list of Partition Key Ranges.
        /// </summary>
        /// <param name="partitionKeyRangesOrCollectionLink">Partition Key Ranges or Collection Self Link.</param>
        /// <param name="options">An instance of <see cref="FeedOptions"/>.</param>
        /// <returns>A list of <see cref="PartitionKeyRange"/>.</returns>
        public async Task<IFeedResponse<PartitionKeyRange>> ReadPartitionKeyRangeFeedAsync(string partitionKeyRangesOrCollectionLink, FeedOptions options)
        {
            return await this.documentClient.ReadPartitionKeyRangeFeedAsync(partitionKeyRangesOrCollectionLink, options).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a query to the Change Feed.
        /// </summary>
        /// <param name="collectionLink">Collection Self Link.</param>
        /// <param name="feedOptions">An instance of <see cref="FeedOptions"/>.</param>
        /// <returns>A query response to transverse.</returns>
        public IChangeFeedDocumentQuery<Document> CreateDocumentChangeFeedQuery(string collectionLink, ChangeFeedOptions feedOptions)
        {
            IDocumentQuery<Document> query = this.documentClient.CreateDocumentChangeFeedQuery(collectionLink, feedOptions);
            return new ChangeFeedDocumentQuery<Document>(query);
        }

        /// <summary>
        /// Reads a database.
        /// </summary>
        /// <param name="databaseUri">Database Uri.</param>
        /// <param name="options">An instance of <see cref="RequestOptions"/>.</param>
        /// <returns>A response containing a <see cref="Database"/>.</returns>
        public async Task<IResourceResponse<Database>> ReadDatabaseAsync(Uri databaseUri, RequestOptions options)
        {
            return await this.documentClient.ReadDatabaseAsync(databaseUri, options).ConfigureAwait(false);
        }

        /// <summary>
        /// Reads a collection.
        /// </summary>
        /// <param name="documentCollectionUri">Collection Uri.</param>
        /// <param name="options">An instance of <see cref="RequestOptions"/>.</param>
        /// <returns>A response containing a <see cref="DocumentCollection"/>.</returns>
        public async Task<IResourceResponse<DocumentCollection>> ReadDocumentCollectionAsync(Uri documentCollectionUri, RequestOptions options)
        {
            return await this.documentClient.ReadDocumentCollectionAsync(documentCollectionUri, options).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a Document.
        /// </summary>
        /// <param name="documentsFeedOrDatabaseLink">Collection Self Link.</param>
        /// <param name="document">Document to store.</param>
        /// <returns>A response containing a <see cref="Document"/>.</returns>
        public async Task<IResourceResponse<Document>> CreateDocumentAsync(string documentsFeedOrDatabaseLink, object document)
        {
            return await this.documentClient.CreateDocumentAsync(documentsFeedOrDatabaseLink, document).ConfigureAwait(false);
        }

        /// <summary>
        /// Deletes a Document
        /// </summary>
        /// <param name="documentUri">Document Uri.</param>
        /// <returns>A response containing a <see cref="Document"/>.</returns>
        public async Task<IResourceResponse<Document>> DeleteDocumentAsync(Uri documentUri)
        {
            return await this.documentClient.DeleteDocumentAsync(documentUri).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates a document.
        /// </summary>
        /// <param name="documentUri">Document Uri of the document to update.</param>
        /// <param name="document">Updated document.</param>
        /// <param name="options">An instance of <see cref="RequestOptions"/>.</param>
        /// <returns>A response containing a <see cref="Document"/>.</returns>
        public async Task<IResourceResponse<Document>> ReplaceDocumentAsync(Uri documentUri, object document, RequestOptions options)
        {
            return await this.documentClient.ReplaceDocumentAsync(documentUri, document, options).ConfigureAwait(false);
        }

        /// <summary>
        /// Reads a single Document.
        /// </summary>
        /// <param name="documentUri">Document Uri to read.</param>
        /// <returns>A response containing a <see cref="Document"/>.</returns>
        public async Task<IResourceResponse<Document>> ReadDocumentAsync(Uri documentUri)
        {
            return await this.documentClient.ReadDocumentAsync(documentUri).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates an instance of <see cref="IQueryable"/>.
        /// </summary>
        /// <typeparam name="T">Document type.</typeparam>
        /// <param name="documentCollectionUri">Collection Uri.</param>
        /// <param name="querySpec">Instace of <see cref="SqlQuerySpec"/> to execute.</param>
        /// <returns>An <see cref="IQueryable"/>.</returns>
        public IQueryable<T> CreateDocumentQuery<T>(string documentCollectionUri, SqlQuerySpec querySpec)
        {
            return this.documentClient.CreateDocumentQuery<T>(documentCollectionUri, querySpec);
        }
    }
}
