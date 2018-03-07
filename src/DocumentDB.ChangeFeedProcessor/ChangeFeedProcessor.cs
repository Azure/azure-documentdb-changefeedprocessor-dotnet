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
        private readonly string hostName;
        private readonly IPartitionManager partitionManager;

        public ChangeFeedProcessor(string hostName, IPartitionManager partitionManager)
        {
            if (string.IsNullOrEmpty(hostName)) throw new ArgumentException(nameof(hostName));
            if (partitionManager == null) throw new ArgumentNullException(nameof(partitionManager));

            this.hostName = hostName;
            this.partitionManager = partitionManager;
        }

        public async Task StartAsync()
        {
            Logger.InfoFormat("Starting processor on host '{0}'", this.hostName);

            await this.partitionManager.StartAsync().ConfigureAwait(false);
        }

        public async Task StopAsync()
        {
            Logger.InfoFormat("Stopping processor on host '{0}'", this.hostName);

            await this.partitionManager.StopAsync().ConfigureAwait(false);
        }
    }
}