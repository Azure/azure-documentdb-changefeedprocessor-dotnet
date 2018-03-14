//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    using Moq;

    using Xunit;

    [Trait("Category", "Gated")]
    public class PartitionLoadBalancerTests
    {
        private readonly ILeaseManager leaseManager = Mock.Of<ILeaseManager>();
        private readonly ILoadBalancingStrategy strategy = Mock.Of<ILoadBalancingStrategy>();

        [Fact]
        public async Task AddLease_ThrowsException_LeaseAddingContinues()
        {
            FailingPartitionController controller = new FailingPartitionController();
            var loadBalancer = new PartitionLoadBalancer(controller, this.leaseManager, this.strategy, TimeSpan.FromHours(1));

            Mock.Get(this.strategy)
                .Setup(s => s.CalculateLeasesToTake(It.IsAny<IEnumerable<ILease>>()))
                .Returns(new[] { Mock.Of<ILease>(), Mock.Of<ILease>() });

            loadBalancer.Start();
            await loadBalancer.StopAsync();

            Mock.Get(this.strategy)
                .Verify(s => s.CalculateLeasesToTake(It.IsAny<IEnumerable<ILease>>()), Times.Once);

            Mock.Get(this.leaseManager)
                .Verify(m => m.ListLeasesAsync(), Times.Once);

            Assert.Equal(2, controller.HitCount);
        }

        private class FailingPartitionController : IPartitionController
        {
            public int HitCount { get; private set; }

            public Task AddLeaseAsync(ILease lease)
            {
                this.HitCount++;
                throw new ArgumentException();
            }

            public Task InitializeAsync()
            {
                return Task.FromResult(false);
            }

            public Task ShutdownAsync()
            {
                return Task.FromResult(false);
            }
        }
    }
}
