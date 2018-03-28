//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading.Tasks;

    /// <summary>
    /// Provides an API to start and stop a <see cref="ChangeFeedProcessor"/> instance created by <see cref="ChangeFeedProcessorBuilder.BuildAsync"/>.
    /// </summary>
    public interface IChangeFeedProcessor
    {
        /// <summary>
        /// Start listening for changes.
        /// </summary>
        /// <returns>A <see cref="Task"/>.</returns>
        Task StartAsync();

        /// <summary>
        /// Stops listening for changes.
        /// </summary>
        /// <returns>A <see cref="Task"/>.</returns>
        Task StopAsync();
    }
}