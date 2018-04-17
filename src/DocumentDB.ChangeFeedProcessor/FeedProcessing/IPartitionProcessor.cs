//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides an API to run processing of certain partition specified by <see cref="IPartitionProcessorFactory"/>.
    /// </summary>
    public interface IPartitionProcessor
    {
        /// <summary>
        /// Perform partition processing.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to be used to stop processing</param>
        /// <returns>A <see cref="Task"/>.</returns>
        Task RunAsync(CancellationToken cancellationToken);
    }
}