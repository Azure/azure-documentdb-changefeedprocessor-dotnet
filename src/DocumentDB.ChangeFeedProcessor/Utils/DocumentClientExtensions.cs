//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Utils
{
    internal static class DocumentClientExtensions
    {
        public static async Task<Document> TryGetDocumentAsync(this IDocumentClientEx client, Uri documentUri)
        {
            try
            {
                return await client.ReadDocumentAsync(documentUri).ConfigureAwait(false);
            }
            catch (DocumentClientException ex)
            {
                if (HttpStatusCode.NotFound != ex.StatusCode)
                {
                    throw;
                }
            }

            return null;
        }

        public static async Task<bool> TryCreateDocumentAsync(this IDocumentClientEx client, string collectionLink, object document)
        {
            try
            {
                await client.CreateDocumentAsync(collectionLink, document).ConfigureAwait(false);
                return true;
            }
            catch (DocumentClientException ex)
            {
                if (HttpStatusCode.Conflict != ex.StatusCode)
                {
                    throw;
                }
            }

            return false;
        }

        public static async Task<DocumentCollection> GetDocumentCollectionAsync(this IDocumentClientEx client, DocumentCollectionInfo collectionInfo)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(collectionInfo.DatabaseName, collectionInfo.CollectionName);
            DocumentCollection collection = await client.ReadDocumentCollectionAsync(collectionUri, new RequestOptions()).ConfigureAwait(false);
            return collection;
        }
    }
}