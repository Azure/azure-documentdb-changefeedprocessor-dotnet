//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;

    internal class LeaseManagerParameters
    {
        internal IChangeFeedDocumentClient Client { get; set; }

        internal IDocumentServiceLeaseUpdater LeaseUpdater { get; set; }

        internal DocumentCollectionInfo LeaseCollectionInfo { get; set; }

        internal string ContainerNamePrefix { get; set; }

        internal string LeaseCollectionLink { get; set; }

        internal string HostName { get; set; }
    }
}
