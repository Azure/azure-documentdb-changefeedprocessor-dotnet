using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Obsolete.Adapters
{
    using System;
    using System.Threading.Tasks;

    internal class ChangeFeedObserverContextAdapter : ChangeFeedObserverContext
    {
        private readonly IChangeFeedObserverContext inner;

        public ChangeFeedObserverContextAdapter(IChangeFeedObserverContext inner)
        {
            if (inner == null) throw new ArgumentNullException(nameof(inner));
            this.inner = inner;

            PartitionKeyRangeId = inner.PartitionKeyRangeId;
            FeedResponse = inner.FeedResponse;
        }

        public override Task CheckpointAsync()
        {
            return this.inner.CheckpointAsync();
        }
    }
}