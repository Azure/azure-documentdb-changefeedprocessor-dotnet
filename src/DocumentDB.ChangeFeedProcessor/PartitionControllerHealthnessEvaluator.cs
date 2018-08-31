//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    internal class PartitionControllerHealthnessEvaluator : IPartitionController
    {
        private static readonly long UninitializedTicks = DateTime.MaxValue.Ticks;
        private readonly IPartitionController inner;
        private readonly long maxAllowedUnhealthyDuration;
        private readonly IUnhealthyHandlingStrategy unhealthyStrategy;
        private long firstUnhealthyTick = UninitializedTicks;

        public PartitionControllerHealthnessEvaluator(IPartitionController inner, TimeSpan leaseExpirationTimeout, IUnhealthyHandlingStrategy unhealthyStrategy)
        {
            this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
            this.unhealthyStrategy = unhealthyStrategy ?? throw new ArgumentNullException(nameof(unhealthyStrategy));
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
            catch (Exception)
            {
                long fistUnhealthyOccurenceTicks = Interlocked.CompareExchange(ref this.firstUnhealthyTick, DateTime.UtcNow.Ticks, UninitializedTicks);
                if (DateTime.UtcNow.Ticks > checked(fistUnhealthyOccurenceTicks + this.maxAllowedUnhealthyDuration))
                {
                    await this.unhealthyStrategy.HandleAsync();
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