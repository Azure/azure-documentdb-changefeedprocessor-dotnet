//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

#if PRIVATE_API
    public
#else
    internal
#endif
    interface IPartitionSupervisor : IDisposable
    {
        Task RunAsync(CancellationToken shutdownToken);
    }
}