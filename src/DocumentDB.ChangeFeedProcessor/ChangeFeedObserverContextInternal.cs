//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// The context passed to <see cref="IChangeFeedObserver"/> events.
    /// </summary>
    internal class ChangeFeedObserverContextInternal : ChangeFeedObserverContext
    {
        private readonly IPartitionCheckpointer checkpointer;

        internal ChangeFeedObserverContextInternal(string partitionId)
        {
            this.PartitionKeyRangeId = partitionId;
        }

        internal ChangeFeedObserverContextInternal(string partitionId, IFeedResponse<Document> feedResponse, IPartitionCheckpointer checkpointer)
        {
            this.PartitionKeyRangeId = partitionId;
            this.FeedResponse = feedResponse;
            this.checkpointer = checkpointer;
        }

        /// <summary>
        /// Checkpoints progress of a stream. This method is valid only if manual checkpoint was configured.
        /// Client may accept multiple change feed batches to process in parallel.
        /// Once first N document processing was finished the client can call checkpoint on the last completed batches in the row.
        /// In case of automatic checkpointing this is method throws.
        /// </summary>
        /// <exception cref="Exceptions.LeaseLostException">Thrown if other host acquired the lease or the lease was deleted</exception>
        public override Task CheckpointAsync()
        {
            return this.checkpointer.CheckpointPartitionAsync(this.FeedResponse.ResponseContinuation);
        }
    }
}