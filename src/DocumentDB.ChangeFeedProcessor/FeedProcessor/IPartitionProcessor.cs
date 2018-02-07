//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    using System.Threading;
    using System.Threading.Tasks;

    public interface IPartitionProcessor
    {
        Task RunAsync(CancellationToken cancellationToken);
    }
}