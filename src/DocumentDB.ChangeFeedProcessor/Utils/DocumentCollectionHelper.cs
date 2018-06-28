//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Utils
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.Client;

    internal static class DocumentCollectionHelper
    {
        private const string DefaultUserAgentSuffix = "changefeed-2.0.4";

        public static DocumentCollectionInfo Canonicalize(this DocumentCollectionInfo collectionInfo)
        {
            DocumentCollectionInfo result = collectionInfo;
            if (string.IsNullOrEmpty(result.ConnectionPolicy.UserAgentSuffix))
            {
                result = new DocumentCollectionInfo(collectionInfo)
                {
                    ConnectionPolicy = { UserAgentSuffix = DefaultUserAgentSuffix },
                };
            }

            return result;
        }

        internal static IChangeFeedDocumentClient CreateDocumentClient(this DocumentCollectionInfo collectionInfo)
        {
            var internalClient = new DocumentClient(collectionInfo.Uri, collectionInfo.MasterKey, collectionInfo.ConnectionPolicy);
            return new ChangeFeedDocumentClient(internalClient);
        }

        internal static string GetCollectionSelfLink(this DocumentCollectionInfo collectionInfo)
        {
            return UriFactory.CreateDocumentCollectionUri(collectionInfo.DatabaseName, collectionInfo.CollectionName).ToString();
        }
    }
}