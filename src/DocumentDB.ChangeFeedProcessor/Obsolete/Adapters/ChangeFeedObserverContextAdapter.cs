//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Obsolete.Adapters
{
    using System;
    using System.Threading.Tasks;

#pragma warning disable CS0618 // Type or member is obsolete
    internal class ChangeFeedObserverContextAdapter : ChangeFeedObserverContext
#pragma warning restore CS0618 // Type or member is obsolete
    {
        private readonly Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverContext inner;

        public ChangeFeedObserverContextAdapter(Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverContext inner)
        {
            if (inner == null) throw new ArgumentNullException(nameof(inner));
            this.inner = inner;

            this.PartitionKeyRangeId = inner.PartitionKeyRangeId;
            this.FeedResponse = inner.FeedResponse;
        }

        public override Task CheckpointAsync()
        {
            return this.inner.CheckpointAsync();
        }
    }
}