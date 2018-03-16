//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Processing
{
    internal interface IPartitionProcessor
    {
        Task RunAsync(CancellationToken cancellationToken);
    }
}