using System;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    internal class ProcessorSettings
    {
        public string CollectionSelfLink;
        public string PartitionKeyRangeId;
        public int? MaxItemCount;
        public string RequestContinuation;
        public TimeSpan FeedPollDelay;
    }
}