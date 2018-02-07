//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    public class PartitionCheckpointer : IPartitionCheckpointer
    {
        private readonly ILeaseManager leaseManager;
        private readonly ILease lease;

        public PartitionCheckpointer(ILeaseManager leaseManager, ILease lease)
        {
            this.leaseManager = leaseManager;
            this.lease = lease;
        }

        public async Task CheckpointPartitionAsync(string сontinuationToken)
        {
            await this.leaseManager.CheckpointAsync(this.lease, сontinuationToken).ConfigureAwait(false);
        }
    }
}