﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    public class ChangeFeedDocumentClient : IChangeFeedDocumentClient
    {
        private readonly DocumentClient documentClient;

        public ChangeFeedDocumentClient(DocumentClient documentClient)
        {
            if (documentClient == null) throw new ArgumentNullException(nameof(documentClient));
            this.documentClient = documentClient;
        }

        public async Task<IFeedResponse<PartitionKeyRange>> ReadPartitionKeyRangeFeedAsync(string partitionKeyRangesOrCollectionLink, FeedOptions options)
        {
            return await this.documentClient.ReadPartitionKeyRangeFeedAsync(partitionKeyRangesOrCollectionLink, options).ConfigureAwait(false);
        }

        public IChangeFeedDocumentQuery<Document> CreateDocumentChangeFeedQuery(string collectionLink, ChangeFeedOptions feedOptions)
        {
            IDocumentQuery<Document> query = this.documentClient.CreateDocumentChangeFeedQuery(collectionLink, feedOptions);
            return new ChangeFeedDocumentQuery<Document>(query);
        }

        public async Task<IResourceResponse<Database>> ReadDatabaseAsync(Uri databaseUri, RequestOptions options)
        {
            return await this.documentClient.ReadDatabaseAsync(databaseUri, options).ConfigureAwait(false);
        }

        public async Task<IResourceResponse<DocumentCollection>> ReadDocumentCollectionAsync(Uri documentCollectionUri, RequestOptions options)
        {
            return await this.documentClient.ReadDocumentCollectionAsync(documentCollectionUri, options).ConfigureAwait(false);
        }

        public async Task<IResourceResponse<Document>> CreateDocumentAsync(string documentsFeedOrDatabaseLink, object document)
        {
            return await this.documentClient.CreateDocumentAsync(documentsFeedOrDatabaseLink, document).ConfigureAwait(false);
        }

        public async Task<IResourceResponse<Document>> DeleteDocumentAsync(Uri documentUri)
        {
            return await this.documentClient.DeleteDocumentAsync(documentUri).ConfigureAwait(false);
        }

        public async Task<IResourceResponse<Document>> ReplaceDocumentAsync(Uri documentUri, object document, RequestOptions options)
        {
            return await this.documentClient.ReplaceDocumentAsync(documentUri, document, options).ConfigureAwait(false);
        }

        public async Task<IResourceResponse<Document>> ReadDocumentAsync(Uri documentUri)
        {
            return await this.documentClient.ReadDocumentAsync(documentUri).ConfigureAwait(false);
        }

        public IQueryable<T> CreateDocumentQuery<T>(string documentCollectionUri, SqlQuerySpec querySpec)
        {
            return this.documentClient.CreateDocumentQuery<T>(documentCollectionUri, querySpec);
        }
    }
}
