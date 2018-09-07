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

    internal class PartitionControllerHealthinessEvaluator : IPartitionController
    {
        private static readonly long UninitializedTicks = DateTime.MaxValue.Ticks;
        private readonly IPartitionController inner;
        private readonly long maxAllowedUnhealthyDuration;
        private readonly IUnhealthinessHandlingStrategy unhealthinessStrategy;
        private long firstUnhealthyTick = UninitializedTicks;

        public PartitionControllerHealthinessEvaluator(IPartitionController inner, TimeSpan leaseExpirationTimeout, IUnhealthinessHandlingStrategy unhealthinessStrategy)
        {
            this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
            this.unhealthinessStrategy = unhealthinessStrategy ?? throw new ArgumentNullException(nameof(unhealthinessStrategy));
            this.maxAllowedUnhealthyDuration = leaseExpirationTimeout.Ticks * 5;
        }

        public async Task AddOrUpdateLeaseAsync(ILease lease)
        {
            try
            {
                await this.inner.AddOrUpdateLeaseAsync(lease);
                Interlocked.CompareExchange(ref this.firstUnhealthyTick, UninitializedTicks, this.firstUnhealthyTick);
            }
            catch (DocumentClientException)
            {
                throw;
            }
            catch (Exception exception)
            {
                long fistUnhealthyOccurenceTicks = Interlocked.CompareExchange(ref this.firstUnhealthyTick, DateTime.UtcNow.Ticks, UninitializedTicks);
                if (DateTime.UtcNow.Ticks > checked(fistUnhealthyOccurenceTicks + this.maxAllowedUnhealthyDuration))
                {
                    await this.unhealthinessStrategy.HandleAsync(lease, exception);
                }

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