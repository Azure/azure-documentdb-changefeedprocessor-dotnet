//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Processing
{
#if PRIVATE_API
    public
#else
    internal
#endif
    interface IPartitionProcessor
    {
        Task RunAsync(CancellationToken cancellationToken);
    }
}