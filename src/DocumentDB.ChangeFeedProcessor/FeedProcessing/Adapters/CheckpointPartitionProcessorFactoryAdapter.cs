//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.Adapters
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class CheckpointPartitionProcessorFactoryAdapter : ICheckpointPartitionProcessorFactory
    {
        private readonly IPartitionProcessorFactory partitionProcessorFactory;

        public CheckpointPartitionProcessorFactoryAdapter(IPartitionProcessorFactory partitionProcessorFactory)
        {
            this.partitionProcessorFactory = partitionProcessorFactory;
        }

        public IPartitionProcessor Create(ILease lease, ILeaseCheckpointer leaseCheckpointer, IChangeFeedObserver observer)
        {
            return this.partitionProcessorFactory.Create(lease, observer);
        }
    }
}
