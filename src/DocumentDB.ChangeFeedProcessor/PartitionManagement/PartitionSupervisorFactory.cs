//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;

    internal class PartitionSupervisorFactory : IPartitionSupervisorFactory
    {
        private readonly IChangeFeedObserverFactory observerFactory;
        private readonly ILeaseManager leaseManager;
        private readonly ChangeFeedProcessorOptions changeFeedProcessorOptions;
        private readonly IPartitionProcessorFactory partitionProcessorFactory;

        public PartitionSupervisorFactory(
            IChangeFeedObserverFactory observerFactory,
            ILeaseManager leaseManager,
            IPartitionProcessorFactory partitionProcessorFactory,
            ChangeFeedProcessorOptions options)
        {
            if (observerFactory == null) throw new ArgumentNullException(nameof(observerFactory));
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (partitionProcessorFactory == null) throw new ArgumentNullException(nameof(partitionProcessorFactory));

            this.observerFactory = observerFactory;
            this.leaseManager = leaseManager;
            this.changeFeedProcessorOptions = options;
            this.partitionProcessorFactory = partitionProcessorFactory;
        }

        public IPartitionSupervisor Create(ILease lease)
        {
            if (lease == null)
                throw new ArgumentNullException(nameof(lease));

            IChangeFeedObserver changeFeedObserver = this.observerFactory.CreateObserver();
            var processor = this.partitionProcessorFactory.Create(lease, changeFeedObserver);
            var renewer = new LeaseRenewer(lease, this.leaseManager, this.changeFeedProcessorOptions.LeaseRenewInterval);

            return new PartitionSupervisor(lease, changeFeedObserver, processor, renewer);
        }
    }
}