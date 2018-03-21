//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess
{
    public interface IChangeFeedDocumentClient
    {
        Task<IFeedResponse<PartitionKeyRange>> ReadPartitionKeyRangeFeedAsync(string partitionKeyRangesOrCollectionLink, FeedOptions options);

        IChangeFeedDocumentQuery<Document> CreateDocumentChangeFeedQuery(string collectionLink, ChangeFeedOptions feedOptions);

        Task<IResourceResponse<Database>> ReadDatabaseAsync(Uri databaseUri, RequestOptions options);

        Task<IResourceResponse<DocumentCollection>> ReadDocumentCollectionAsync(Uri documentCollectionUri, RequestOptions options);

        Task<IResourceResponse<Document>> CreateDocumentAsync(string documentsFeedOrDatabaseLink, object document);

        Task<IResourceResponse<Document>> DeleteDocumentAsync(Uri documentUri);

        Task<IResourceResponse<Document>> ReplaceDocumentAsync(Uri documentUri, object document, RequestOptions options);

        Task<IResourceResponse<Document>> ReadDocumentAsync(Uri documentUri);

        IQueryable<T> CreateDocumentQuery<T>(string documentCollectionUri, SqlQuerySpec querySpec);
    }
}