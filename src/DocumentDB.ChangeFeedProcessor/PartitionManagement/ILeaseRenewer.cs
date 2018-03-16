//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading;
    using System.Threading.Tasks;

    internal interface ILeaseRenewer
    {
        /// <summary>
        /// Starts the lease renewer
        /// </summary>
        Task RunAsync(CancellationToken cancellationToken);
    }
}