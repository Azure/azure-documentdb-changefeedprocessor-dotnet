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
        Task<FeedResponse<PartitionKeyRange>> ReadPartitionKeyRangeFeedAsync(string partitionKeyRangesOrCollectionLink, FeedOptions options);

        IChangeFeedDocumentQuery<Document> CreateDocumentChangeFeedQuery(string collectionLink, ChangeFeedOptions feedOptions);

        Task<ResourceResponse<Database>> ReadDatabaseAsync(Uri databaseUri, RequestOptions options);

        Task<ResourceResponse<DocumentCollection>> ReadDocumentCollectionAsync(Uri documentCollectionUri, RequestOptions options);

        Task<ResourceResponse<Document>> CreateDocumentAsync(string documentsFeedOrDatabaseLink, object document);

        Task<ResourceResponse<Document>> DeleteDocumentAsync(Uri documentUri);

        Task<ResourceResponse<Document>> ReplaceDocumentAsync(Uri documentUri, object document, RequestOptions options);

        Task<ResourceResponse<Document>> ReadDocumentAsync(Uri documentUri);

        IQueryable<T> CreateDocumentQuery<T>(string documentCollectionUri, SqlQuerySpec querySpec);
    }
}