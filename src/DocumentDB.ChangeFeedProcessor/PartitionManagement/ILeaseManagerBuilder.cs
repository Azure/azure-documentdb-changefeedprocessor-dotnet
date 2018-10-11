//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;

    internal interface ILeaseManagerBuilder
    {
        Task<ILeaseManager> BuildAsync();

        LeaseManagerBuilder WithLeaseCollection(DocumentCollectionInfo leaseCollectionLocation);

        LeaseManagerBuilder WithLeaseCollectionMetadata(CollectionMetadata leaseCollectionMetadata);

        LeaseManagerBuilder WithLeaseDocumentClient(IChangeFeedDocumentClient leaseDocumentClient);

        LeaseManagerBuilder WithLeasePrefix(string leasePrefix);
    }
}