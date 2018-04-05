//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// The context passed to <see cref="IChangeFeedObserver"/> events.
    /// </summary>
    [Obsolete("Switch to the ChangeFeedProcessorBuilder for building the change feed processor and use new interface Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverContext.")]
    public abstract class ChangeFeedObserverContext
    {
        /// <summary>
        /// Gets or sets the id of the partition for current event.
        /// </summary>
        public string PartitionKeyRangeId { get; protected set; }

        /// <summary>
        /// Gets or sets the response from the underlying <see cref="Microsoft.Azure.Documents.Linq.IDocumentQuery&lt;T&gt;.ExecuteNextAsync"/> call.
        /// </summary>
        public IFeedResponse<Document> FeedResponse { get; protected set; }

        /// <summary>
        /// Checkpoints progress of a stream. This method is valid only if manual checkpoint was configured.
        /// Client may accept multiple change feed batches to process in parallel.
        /// Once first N document processing was finished the client can call checkpoint on the last completed batches in the row.
        /// In case of automatic checkpointing this is method throws.
        /// </summary>
        /// <exception cref="Exceptions.LeaseLostException">Thrown if other host acquired the lease or the lease was deleted</exception>
        /// <returns>A <see cref="Task"/>.</returns>
        public abstract Task CheckpointAsync();
    }
}