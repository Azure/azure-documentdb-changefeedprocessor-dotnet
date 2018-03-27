//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading.Tasks;

    /// <summary>
    /// Listens for changes in a monitored collection and acts upon them.
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