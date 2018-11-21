//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;

    internal class ProcessorSettings
    {
        public string CollectionSelfLink { get; set; }

        public string PartitionKeyRangeId { get; set; }

        public int? MaxItemCount { get; set; }

        public TimeSpan FeedPollDelay { get; set; }

        public string StartContinuation { get; set; }

        public bool StartFromBeginning { get; set; }

        public DateTime? StartTime { get; set; }

        public string SessionToken { get; set; }
    }
}