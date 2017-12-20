//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionManagerBuilder
    {
        Task<IPartitionManager> BuildPartitionManagerAsync(string hostName, string leasePrefix, IChangeFeedObserverFactory observerFactory,
            IDocumentClientEx feedDocumentClient, DocumentCollectionInfo feedCollectionInfo, ChangeFeedOptions options, ChangeFeedHostOptions hostOptions);
    }
}