//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Represents the context passed to <see cref="IChangeFeedObserver"/> events.
    /// </summary>
    public interface IChangeFeedObserverContext
    {
        /// <summary>
        /// Gets the id of the partition for the current event.
        /// </summary>
        string PartitionKeyRangeId { get; }

        /// <summary>
        /// Gets the response from the underlying <see cref="Microsoft.Azure.Documents.Linq.IDocumentQuery&lt;T&gt;.ExecuteNextAsync"/> call.
        /// </summary>
        IFeedResponse<Document> FeedResponse { get; }

        /// <summary>
        /// Checkpoints progress of a stream. This method is valid only if manual checkpoint was configured.
        /// Client may accept multiple change feed batches to process in parallel.
        /// Once first N document processing was finished the client can call checkpoint on the last completed batches in the row.
        /// In case of automatic checkpointing this is method throws.
        /// </summary>
        /// <exception cref="Exceptions.LeaseLostException">Thrown if other host acquired the lease or the lease was deleted</exception>
        Task CheckpointAsync();
    }
}