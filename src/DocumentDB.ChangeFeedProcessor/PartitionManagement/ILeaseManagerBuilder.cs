//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface ILeaseManagerBuilder
    {
        Task<ILeaseManager> BuildAsync();
        LeaseManagerBuilder WithLeaseCollection(DocumentCollectionInfo leaseCollectionLocation);
        LeaseManagerBuilder WithLeaseCollectionLink(string leaseStoreCollectionLink);
        LeaseManagerBuilder WithLeaseDocumentClient(IDocumentClientEx leaseDocumentClient);
        LeaseManagerBuilder WithLeasePrefix(string leasePrefix);
    }
}