//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class ChangeFeedProcessor : IChangeFeedProcessor
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly IPartitionManager partitionManager;

        public ChangeFeedProcessor(IPartitionManager partitionManager)
        {
            if (partitionManager == null) throw new ArgumentNullException(nameof(partitionManager));
            this.partitionManager = partitionManager;
        }

        public async Task StartAsync()
        {
            Logger.InfoFormat("Starting processor...");
            await this.partitionManager.StartAsync().ConfigureAwait(false);
            Logger.InfoFormat("Processor started.");
        }

        public async Task StopAsync()
        {
            Logger.InfoFormat("Stopping processor...");
            await this.partitionManager.StopAsync().ConfigureAwait(false);
            Logger.InfoFormat("Processor stopped.");
        }
    }
}