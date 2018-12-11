//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    /// <summary>
    /// Adapts <see cref="DocumentClient"/> to <see cref="IChangeFeedDocumentClient"/>.
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
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.FeedOptions"/>for this request.</param>
        /// <returns>A list of <see cref="PartitionKeyRange"/>.</returns>
        public async Task<IFeedResponse<PartitionKeyRange>> ReadPartitionKeyRangeFeedAsync(string partitionKeyRangesOrCollectionLink, FeedOptions options = null)
        {
            return await this.documentClient.ReadPartitionKeyRangeFeedAsync(partitionKeyRangesOrCollectionLink, options).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a query to the Change Feed.
        /// </summary>
        /// <param name="collectionLink">Collection Self Link.</param>
        /// <param name="feedOptions">An instance of <see cref="FeedOptions"/>.</param>
        /// <returns>A query response to transverse.</returns>
        public IChangeFeedDocumentQuery<Document> CreateDocumentChangeFeedQuery(string collectionLink, ChangeFeedOptions feedOptions = null)
        {
            IDocumentQuery<Document> query = this.documentClient.CreateDocumentChangeFeedQuery(collectionLink, feedOptions);
            return new ChangeFeedDocumentQuery<Document>(query);
        }

        /// <summary>
        /// Reads the database.
        /// </summary>
        /// <param name="databaseUri">Database Uri.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <returns>A response containing a <see cref="Database"/>.</returns>
        public async Task<IResourceResponse<Database>> ReadDatabaseAsync(Uri databaseUri, RequestOptions options = null)
        {
            return await this.documentClient.ReadDatabaseAsync(databaseUri, options).ConfigureAwait(false);
        }

        /// <summary>
        /// Reads the collection.
        /// </summary>
        /// <param name="documentCollectionUri">Collection Uri.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <returns>A response containing a <see cref="DocumentCollection"/>.</returns>
        public async Task<IResourceResponse<DocumentCollection>> ReadDocumentCollectionAsync(Uri documentCollectionUri, RequestOptions options = null)
        {
            return await this.documentClient.ReadDocumentCollectionAsync(documentCollectionUri, options).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates the document.
        /// </summary>
        /// <param name="documentsFeedOrDatabaseLink">Collection Self Link.</param>
        /// <param name="document">Document to store.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <param name="disableAutomaticIdGeneration">Disables the automatic id generation, If this is True the system will throw an exception if the id property is missing from the Document.</param>
        /// <param name="cancellationToken">A <see cref="System.Threading.CancellationToken"/>that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>A response containing a <see cref="Document"/>.</returns>
        public async Task<IResourceResponse<Document>> CreateDocumentAsync(
            string documentsFeedOrDatabaseLink,
            object document,
            RequestOptions options = null,
            bool disableAutomaticIdGeneration = false,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return await this.documentClient.CreateDocumentAsync(
                documentsFeedOrDatabaseLink,
                document,
                options,
                disableAutomaticIdGeneration,
                cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Deletes the document.
        /// </summary>
        /// <param name="documentUri">Document Uri.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <param name="cancellationToken">A <see cref="System.Threading.CancellationToken"/>that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>A response containing a <see cref="Document"/>.</returns>
        public async Task<IResourceResponse<Document>> DeleteDocumentAsync(
            Uri documentUri,
            RequestOptions options = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return await this.documentClient.DeleteDocumentAsync(documentUri, options, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates the document.
        /// </summary>
        /// <param name="documentUri">Document Uri of the document to update.</param>
        /// <param name="document">Updated document.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <param name="cancellationToken">A <see cref="System.Threading.CancellationToken"/>that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>A response containing a <see cref="Document"/>.</returns>
        public async Task<IResourceResponse<Document>> ReplaceDocumentAsync(
            Uri documentUri,
            object document,
            RequestOptions options = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return await this.documentClient.ReplaceDocumentAsync(documentUri, document, options, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Reads a single document.
        /// </summary>
        /// <param name="documentUri">Document Uri to read.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <param name="cancellationToken">A <see cref="System.Threading.CancellationToken"/>that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>A response containing a <see cref="Document"/>.</returns>
        public async Task<IResourceResponse<Document>> ReadDocumentAsync(
            Uri documentUri,
            RequestOptions options = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return await this.documentClient.ReadDocumentAsync(documentUri, options, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates an instance of <see cref="IQueryable"/>.
        /// </summary>
        /// <typeparam name="T">Document type.</typeparam>
        /// <param name="documentCollectionUri">Collection Uri.</param>
        /// <param name="querySpec">Instance of <see cref="SqlQuerySpec"/> to execute.</param>
        /// <param name="feedOptions">Options for the query.</param>
        /// <returns>An <see cref="IQueryable"/>.</returns>
        public IQueryable<T> CreateDocumentQuery<T>(string documentCollectionUri, SqlQuerySpec querySpec, FeedOptions feedOptions = null)
        {
            return this.documentClient.CreateDocumentQuery<T>(documentCollectionUri, querySpec, feedOptions);
        }

        /// <summary>
        /// Reads the list of Offers.
        /// </summary>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.FeedOptions"/>for this request.</param>
        /// <returns>A list of <see cref="Offer"/>.</returns>
        public async Task<IFeedResponse<Offer>> ReadOffersFeedAsync(FeedOptions options = null)
        {
            return await this.documentClient.ReadOffersFeedAsync(options).ConfigureAwait(false);
        }
    }
}
