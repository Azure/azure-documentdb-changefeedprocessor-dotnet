//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionManagerBuilder
    {
        Task<IChangeFeedProcessor> BuildPartitionManagerAsync(string hostName, string leasePrefix, IChangeFeedObserverFactory observerFactory,
            IDocumentClientEx feedDocumentClient, DocumentCollectionInfo feedCollectionInfo, ChangeFeedOptions feedOptions, ChangeFeedHostOptions options);
    }
}