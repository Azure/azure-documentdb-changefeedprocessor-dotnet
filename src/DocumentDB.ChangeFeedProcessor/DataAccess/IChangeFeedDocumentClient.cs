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

    /// <summary>
    /// The IDocumentClient interface captures the API required to handle change feed processing logic
    /// </summary>
    public interface IChangeFeedDocumentClient
    {
        /// <summary>
        /// Reads the feed (sequence) of <see cref="Microsoft.Azure.Documents.PartitionKeyRange"/> for a database account from the Azure Cosmos DB service as an asynchronous operation.
        /// </summary>
        /// <param name="partitionKeyRangesOrCollectionLink">The link of the resources to be read, or owner collection link, SelfLink or AltLink. E.g. /dbs/db_rid/colls/coll_rid/pkranges</param>
        /// <param name="options">The request options for the request.</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which wraps a <see cref="Microsoft.Azure.Documents.Database"/> containing the read resource record.
        /// </returns>
        Task<IFeedResponse<PartitionKeyRange>> ReadPartitionKeyRangeFeedAsync(string partitionKeyRangesOrCollectionLink, FeedOptions options = null);

        /// <summary>
        /// Method to create a change feed query for documents.
        /// </summary>
        /// <param name="collectionLink">Specifies the collection to read documents from.</param>
        /// <param name="feedOptions">The options for processing the query results feed.</param>
        /// <returns>the query result set.</returns>
        IChangeFeedDocumentQuery<Document> CreateDocumentChangeFeedQuery(string collectionLink, ChangeFeedOptions feedOptions);

        /// <summary>
        /// Reads a <see cref="Microsoft.Azure.Documents.Database"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="databaseUri">The link of the Database resource to be read.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which wraps a <see cref="Microsoft.Azure.Documents.Database"/> containing the read resource record.
        /// </returns>
        Task<IResourceResponse<Database>> ReadDatabaseAsync(Uri databaseUri, RequestOptions options = null);

        /// <summary>
        /// Reads a <see cref="Microsoft.Azure.Documents.DocumentCollection"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="documentCollectionUri">The link for the DocumentCollection to be read.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which wraps a <see cref="Microsoft.Azure.Documents.DocumentCollection"/> containing the read resource record.
        /// </returns>
        Task<IResourceResponse<DocumentCollection>> ReadDocumentCollectionAsync(Uri documentCollectionUri, RequestOptions options = null);

        /// <summary>
        /// Creates a Document as an asychronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="collectionLink">The link of the <see cref="Microsoft.Azure.Documents.DocumentCollection"/> to create the document in. E.g. dbs/db_rid/colls/coll_rid/ </param>
        /// <param name="document">The document object to create.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <param name="disableAutomaticIdGeneration">Disables the automatic id generation, If this is True the system will throw an exception if the id property is missing from the Document.</param>
        /// <param name="cancellationToken">A <see cref="System.Threading.CancellationToken"/>that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which will contain information about the request issued.</returns>
        Task<IResourceResponse<Document>> CreateDocumentAsync(
            string collectionLink,
            object document,
            RequestOptions options = null,
            bool disableAutomaticIdGeneration = false,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Delete a <see cref="Microsoft.Azure.Documents.Document"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="documentUri">The link of the <see cref="Microsoft.Azure.Documents.Document"/> to delete. E.g. dbs/db_rid/colls/col_rid/docs/doc_rid/ </param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <param name="cancellationToken">A <see cref="System.Threading.CancellationToken"/>that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which will contain information about the request issued.</returns>
        Task<IResourceResponse<Document>> DeleteDocumentAsync(
            Uri documentUri,
            RequestOptions options = null,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Replaces a <see cref="Microsoft.Azure.Documents.Document"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="documentUri">The link of the document to be updated. E.g. dbs/db_rid/colls/col_rid/docs/doc_rid/ </param>
        /// <param name="document">The updated <see cref="Microsoft.Azure.Documents.Document"/> to replace the existing resource with.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <param name="cancellationToken">A <see cref="System.Threading.CancellationToken"/>that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which wraps a <see cref="Microsoft.Azure.Documents.Document"/> containing the updated resource record.
        /// </returns>
        Task<IResourceResponse<Document>> ReplaceDocumentAsync(
            Uri documentUri,
            object document,
            RequestOptions options = null,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Reads a <see cref="Microsoft.Azure.Documents.Document"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="documentUri">The link for the document to be read.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <param name="cancellationToken">A <see cref="System.Threading.CancellationToken"/>that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which wraps a <see cref="Microsoft.Azure.Documents.Document"/> containing the read resource record.
        /// </returns>
        Task<IResourceResponse<Document>> ReadDocumentAsync(
            Uri documentUri,
            RequestOptions options = null,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Extension method to create a query for documents in the Azure DocumentDB database service.
        /// </summary>
        /// <typeparam name="T">The type of object to query.</typeparam>
        /// <param name="documentCollectionUri">The URI of the document collection.</param>
        /// <param name="querySpec">The sql query.</param>
        /// <param name="feedOptions">Options for the query.</param>
        /// <returns>The query result set.</returns>
        IQueryable<T> CreateDocumentQuery<T>(string documentCollectionUri, SqlQuerySpec querySpec, FeedOptions feedOptions = null);

        /// <summary>
        /// Reads the feed (sequence) of <see cref="Offer"/> for the database account.
        /// </summary>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.FeedOptions"/>for this request.</param>
        /// <returns>A list of <see cref="Offer"/>.</returns>
        Task<IFeedResponse<Offer>> ReadOffersFeedAsync(FeedOptions options = null);
    }
}