﻿//----------------------------------------------------------------
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
        private readonly IParitionLoadBalancingStrategy strategy = Mock.Of<IParitionLoadBalancingStrategy>();

        [Fact]
        public async Task AddLease_ThrowsException_LeaseAddingContinues()
        {
            FailingPartitionController controller = new FailingPartitionController();

            // long acquire interval to ensure that only 1 load balancing iteration is performed in a test run
            var leaseAcquireInterval = TimeSpan.FromHours(1);
            var loadBalancer = new PartitionLoadBalancer(controller, this.leaseManager, this.strategy, leaseAcquireInterval);

            Mock.Get(this.strategy)
                .Setup(s => s.SelectLeasesToTake(It.IsAny<IEnumerable<ILease>>()))
                .Returns(new[] { Mock.Of<ILease>(), Mock.Of<ILease>() });

            Mock.Get(this.leaseManager)
                .Setup(m => m.ListLeasesAsync())
                .ReturnsAsync(new[] { Mock.Of<ILease>(), Mock.Of<ILease>() });

            loadBalancer.Start();
            await loadBalancer.StopAsync();

            Mock.Get(this.strategy)
                .Verify(s => s.SelectLeasesToTake(It.IsAny<IEnumerable<ILease>>()), Times.Once);

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
                return Task.CompletedTask;
            }

            public Task ShutdownAsync()
            {
                return Task.CompletedTask;
            }
        }
    }
}
