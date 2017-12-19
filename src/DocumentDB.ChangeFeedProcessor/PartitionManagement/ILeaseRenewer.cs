//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface ILeaseRenewer
    {
        /// <summary>
        /// Starts the lease renewer
        /// </summary>
        Task RunAsync(CancellationToken cancellationToken);
    }
}