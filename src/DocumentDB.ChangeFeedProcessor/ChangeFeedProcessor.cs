﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class ChangeFeedProcessor : IChangeFeedProcessor
    {
        private readonly IPartitionManager partitionManager;

        public ChangeFeedProcessor(IPartitionManager partitionManager)
        {
            if (partitionManager == null) throw new ArgumentNullException(nameof(partitionManager));

            this.partitionManager = partitionManager;
        }

        public async Task StartAsync()
        {
            await this.partitionManager.StartAsync().ConfigureAwait(false);
        }

        public async Task StopAsync()
        {
            await this.partitionManager.StopAsync().ConfigureAwait(false);
        }
    }
}