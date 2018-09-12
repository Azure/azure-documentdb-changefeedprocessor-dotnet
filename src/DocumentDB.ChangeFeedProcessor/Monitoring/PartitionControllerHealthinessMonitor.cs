//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class PartitionControllerHealthinessMonitor : IPartitionController
    {
        private readonly IPartitionController inner;
        private readonly IHealthinessMonitor monitor;

        public PartitionControllerHealthinessMonitor(IPartitionController inner, IHealthinessMonitor monitor)
        {
            this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
            this.monitor = monitor ?? throw new ArgumentNullException(nameof(monitor));
        }

        public async Task AddOrUpdateLeaseAsync(ILease lease)
        {
            try
            {
                await this.inner.AddOrUpdateLeaseAsync(lease);
                await this.monitor.InspectAsync(HealthEventLevel.Health, HealthEventPhase.AquireLease, lease);
            }
            catch (DocumentClientException)
            {
                throw;
            }
            catch (Exception exception)
            {
                await this.monitor.InspectAsync(HealthEventLevel.Error, HealthEventPhase.AquireLease, lease, exception);

                throw;
            }
        }

        public Task InitializeAsync()
        {
            return this.inner.InitializeAsync();
        }

        public Task ShutdownAsync()
        {
            return this.inner.ShutdownAsync();
        }
    }
}