//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess
{
    using System;
    using System.Linq;
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
        Task<IFeedResponse<PartitionKeyRange>> ReadPartitionKeyRangeFeedAsync(string partitionKeyRangesOrCollectionLink, FeedOptions options);

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
        Task<IResourceResponse<Database>> ReadDatabaseAsync(Uri databaseUri, RequestOptions options);

        /// <summary>
        /// Reads a <see cref="Microsoft.Azure.Documents.DocumentCollection"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="documentCollectionUri">The link for the DocumentCollection to be read.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which wraps a <see cref="Microsoft.Azure.Documents.DocumentCollection"/> containing the read resource record.
        /// </returns>
        Task<IResourceResponse<DocumentCollection>> ReadDocumentCollectionAsync(Uri documentCollectionUri, RequestOptions options);

        /// <summary>
        /// Creates a Document as an asychronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="collectionLink">The link of the <see cref="Microsoft.Azure.Documents.DocumentCollection"/> to create the document in. E.g. dbs/db_rid/colls/coll_rid/ </param>
        /// <param name="document">The document object to create.</param>
        /// <returns>A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which will contain information about the request issued.</returns>
        Task<IResourceResponse<Document>> CreateDocumentAsync(string collectionLink, object document);

        /// <summary>
        /// Delete a <see cref="Microsoft.Azure.Documents.Document"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="documentUri">The link of the <see cref="Microsoft.Azure.Documents.Document"/> to delete. E.g. dbs/db_rid/colls/col_rid/docs/doc_rid/ </param>
        /// <returns>A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which will contain information about the request issued.</returns>
        Task<IResourceResponse<Document>> DeleteDocumentAsync(Uri documentUri);

        /// <summary>
        /// Replaces a <see cref="Microsoft.Azure.Documents.Document"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="documentUri">The link of the document to be updated. E.g. dbs/db_rid/colls/col_rid/docs/doc_rid/ </param>
        /// <param name="document">The updated <see cref="Microsoft.Azure.Documents.Document"/> to replace the existing resource with.</param>
        /// <param name="options">The <see cref="Microsoft.Azure.Documents.Client.RequestOptions"/>for this request.</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which wraps a <see cref="Microsoft.Azure.Documents.Document"/> containing the updated resource record.
        /// </returns>
        Task<IResourceResponse<Document>> ReplaceDocumentAsync(Uri documentUri, object document, RequestOptions options);

        /// <summary>
        /// Reads a <see cref="Microsoft.Azure.Documents.Document"/> as an asynchronous operation in the Azure DocumentDB database service.
        /// </summary>
        /// <param name="documentUri">The link for the document to be read.</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks"/> containing a <see cref="Microsoft.Azure.Documents.Client.IResourceResponse{T}"/> which wraps a <see cref="Microsoft.Azure.Documents.Document"/> containing the read resource record.
        /// </returns>
        Task<IResourceResponse<Document>> ReadDocumentAsync(Uri documentUri);

        /// <summary>
        /// Extension method to create a query for documents in the Azure DocumentDB database service.
        /// </summary>
        /// <typeparam name="T">The type of object to query.</typeparam>
        /// <param name="documentCollectionUri">The URI of the document collection.</param>
        /// <param name="querySpec">The sql query.</param>
        /// <returns>The query result set.</returns>
        IQueryable<T> CreateDocumentQuery<T>(string documentCollectionUri, SqlQuerySpec querySpec);
    }
}