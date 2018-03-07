//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading;
    using System.Threading.Tasks;

#if PRIVATE_API
    public
#else
    internal
#endif
    interface ILeaseRenewer
    {
        /// <summary>
        /// Starts the lease renewer
        /// </summary>
        Task RunAsync(CancellationToken cancellationToken);
    }
}