using System;

namespace DocumentDB.ChangeFeedProcessor.FeedProcessor
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