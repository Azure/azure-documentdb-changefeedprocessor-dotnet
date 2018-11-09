//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    internal class DocumentServiceLeaseStoreManagerSettings
    {
        internal DocumentCollectionInfo LeaseCollectionInfo { get; set; }

        internal string ContainerNamePrefix { get; set; }

        internal string LeaseCollectionLink { get; set; }

        internal string HostName { get; set; }
    }
}
