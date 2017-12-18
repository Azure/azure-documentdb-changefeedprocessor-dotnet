using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Utils
{
    internal static class DocumentCollectionHelper
    {
        private const string DefaultUserAgentSuffix = "changefeed-0.3.3"; // TODO: replace with assemblyVersion?

        public static DocumentCollectionInfo Canonicalize(this DocumentCollectionInfo collectionInfo)
        {
            DocumentCollectionInfo result = collectionInfo;
            if (string.IsNullOrEmpty(result.ConnectionPolicy.UserAgentSuffix))
            {
                result = new DocumentCollectionInfo(collectionInfo)
                {
                    ConnectionPolicy = { UserAgentSuffix = DefaultUserAgentSuffix }
                };
            }

            return result;
        }

        internal static IDocumentClientEx CreateDocumentClient(this DocumentCollectionInfo collectionInfo)
        {
            var internalClient = new DocumentClient(collectionInfo.Uri, collectionInfo.MasterKey, collectionInfo.ConnectionPolicy);
            return new DocumentClientEx(internalClient);
        }

        internal static string GetCollectionSelfLink(this DocumentCollectionInfo collectionInfo)
        {
            return UriFactory.CreateDocumentCollectionUri(collectionInfo.DatabaseName, collectionInfo.CollectionName).ToString();
        }
    }
}