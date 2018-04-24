//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides an API to run continious processing on a single partition of some resource.
    /// Created by <see cref="IPartitionProcessorFactory.Create"/> after some lease is acquired by the current host.
    /// Processing can perform the following tasks in a loop:
    ///   1. Read some data from the resource partition.
    ///   2. Handle possible problems with the read.
    ///   3. Pass the obtained data to an observer by calling <see cref="IChangeFeedObserver.ProcessChangesAsync"/> with the context <see cref="IChangeFeedObserverContext"/>.
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