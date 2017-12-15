using System.Threading.Tasks;
using DocumentDB.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace DocumentDB.ChangeFeedProcessor
{
    /// <summary>
    /// The context passed to <see cref="IChangeFeedObserver"/> events.
    /// </summary>
    public class ChangeFeedObserverContext : IChangeFeedObserverContext
    {
        private readonly IPartitionCheckpointer checkpointer;

        internal ChangeFeedObserverContext(string partitionId)
        {
            PartitionKeyRangeId = partitionId;
        }

        internal ChangeFeedObserverContext(string partitionId, IFeedResponse<Document> feedResponse, IPartitionCheckpointer checkpointer)
        {
            PartitionKeyRangeId = partitionId;
            FeedResponse = feedResponse;
            this.checkpointer = checkpointer;
        }

        public string PartitionKeyRangeId { get; }

        public IFeedResponse<Document> FeedResponse { get; }

        public Task CheckpointAsync()
        {
            return checkpointer.CheckpointPartitionAsync(FeedResponse.ResponseContinuation);
        }
    }
}