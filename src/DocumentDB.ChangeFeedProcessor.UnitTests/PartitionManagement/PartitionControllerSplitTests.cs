﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class PartitionControllerSplitTests : IAsyncLifetime
    {
        private readonly ILease lease, leaseChild, leaseChild2;
        private readonly ILeaseManager leaseManager;
        private readonly IPartitionSupervisorFactory partitionSupervisorFactory;
        private readonly IPartitionSynchronizer synchronizer;
        private readonly PartitionController sut;

        public PartitionControllerSplitTests()
        {
            lease = Mock.Of<ILease>();
            Mock.Get(lease)
                .Setup(l => l.PartitionId)
                .Returns("partitionId");

            leaseChild = Mock.Of<ILease>();
            Mock.Get(leaseChild)
                .Setup(l => l.PartitionId)
                .Returns("childPartitionId");

            leaseChild2 = Mock.Of<ILease>();
            Mock.Get(leaseChild2)
                .Setup(l => l.PartitionId)
                .Returns("childPartitionId2");

            var partitionSupervisor = Mock.Of<IPartitionSupervisor>();
            Mock.Get(partitionSupervisor)
                .Setup(o => o.RunAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new PartitionSplitException("lastContinuation"));

            partitionSupervisorFactory = Mock.Of<IPartitionSupervisorFactory>(f => f.Create(lease) == partitionSupervisor);

            leaseManager = Mock.Of<ILeaseManager>();
            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(lease))
                .ReturnsAsync(lease);

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.FromResult(false));

            synchronizer = Mock.Of<IPartitionSynchronizer>();
            sut = new PartitionController(leaseManager, partitionSupervisorFactory, synchronizer);
        }

        [Fact]
        public async Task Controller_ShouldSignalSynchronizerSplitPartition_IfObserverThrowsPartitionSplitException()
        {
            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild))
                .Returns(new PartitionSupervisor(leaseChild, MockObserver(), MockPartitionProcessor(), MockRenewer()));

            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild2))
                .Returns<ILease>(l => new PartitionSupervisor(l, MockObserver(), MockPartitionProcessor(), MockRenewer()));

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(leaseChild))
                .ReturnsAsync(leaseChild);

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(leaseChild2))
                .ReturnsAsync(leaseChild2);

            Mock.Get(leaseManager)
                .Setup(manager => manager.DeleteAsync(lease))
                .Returns(Task.FromResult(false));

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(leaseChild))
                .Returns(Task.FromResult(false));

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(leaseChild2))
                .Returns(Task.FromResult(false));

            Mock.Get(synchronizer)
                .Setup(s => s.SplitPartitionAsync(lease))
                .ReturnsAsync(new[] { leaseChild, leaseChild2 });

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);
        }

        [Fact]
        public async Task Controller_ShouldCopyParentLeaseProperties_IfObserverThrowsPartitionSplitException()
        {
            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild))
                .Returns(new PartitionSupervisor(leaseChild, MockObserver(), MockPartitionProcessor(), MockRenewer()));

            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild2))
                .Returns<ILease>(l => new PartitionSupervisor(l, MockObserver(), MockPartitionProcessor(), MockRenewer()));

            var customProperties = new Dictionary<string, string> { {"key", "value"} };
            Mock.Get(lease)
                .Setup(l => l.Properties)
                .Returns(customProperties);

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(leaseChild))
                .ReturnsAsync(leaseChild);

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(leaseChild2))
                .ReturnsAsync(leaseChild2);

            Mock.Get(leaseManager)
                .Setup(manager => manager.DeleteAsync(lease))
                .Returns(Task.FromResult(false));

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(leaseChild))
                .Returns(Task.FromResult(false));

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(leaseChild2))
                .Returns(Task.FromResult(false));

            Mock.Get(synchronizer)
                .Setup(s => s.SplitPartitionAsync(lease))
                .ReturnsAsync(new[] { leaseChild, leaseChild2 });

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            Mock.Get(leaseChild)
                .VerifySet(l => l.Properties = customProperties, Times.Once);

            Mock.Get(leaseChild2)
                .VerifySet(l => l.Properties = customProperties, Times.Once);
        }

        // TODO: update the test
        /*
        [Fact]
        public async Task Controller_ShouldProcessChildPartitions_IfParentPartitionIsSplit()
        {
            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild))
                .Returns<ILease>(l => new PartitionSupervisor(l, MockObserver(), MockPartitionProcessor(), "partition", MockRenewer()));

            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild2))
                .Returns<ILease>(l => new PartitionSupervisor(l, MockObserver(), MockPartitionProcessor(), "partition", MockRenewer()));

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(leaseChild, "host"))
                .ReturnsAsync(lease);

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(leaseChild2, "host"))
                .ReturnsAsync(lease);

            Mock.Get(leaseManager)
                .Setup(manager => manager.DeleteAsync(lease))
                .Returns(Task.FromResult(false));

            Mock.Get(synchronizer)
                .Setup(s => s.SplitPartitionAsync(lease))
                .ReturnsAsync(new[] { leaseChild, leaseChild2 });

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            IEnumerable<ILease> leases = sut.GetOwnedLeasesSnapshot();
            Assert.Equal(leases.OrderBy(l => l.PartitionId), new[] { leaseChild, leaseChild2 });
        }
        */

        [Fact]
        public async Task Controller_ShouldKeepParentLease_IfSplitThrows()
        {
            Mock.Get(synchronizer)
                .Setup(s => s.SplitPartitionAsync(lease))
                .ThrowsAsync(new InvalidOperationException());

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            Mock.Get(leaseManager)
                .Verify(manager => manager.DeleteAsync(lease), Times.Never);
        }

        [Fact]
        public async Task Controller_ShouldIgnoreProcessingChildPartition_IfPartitionAlreadyAdded()
        {
            var processor = MockPartitionProcessor();
            Mock.Get(synchronizer)
                .Setup(s => s.SplitPartitionAsync(lease))
                .ReturnsAsync(new[] { leaseChild, leaseChild2 });

            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild))
                .Returns<ILease>(l => new PartitionSupervisor(l, MockObserver(), MockPartitionProcessor(), MockRenewer()));

            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild2))
                .Returns<ILease>(l => new PartitionSupervisor(l, MockObserver(), MockPartitionProcessor(), MockRenewer()));

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);
            await sut.AddOrUpdateLeaseAsync(leaseChild2).ConfigureAwait(false);

            Mock.Get(leaseManager)
                .Verify(manager => manager.AcquireAsync(leaseChild2), Times.Once);

            Mock.Get(leaseManager)
                .Verify(manager => manager.UpdatePropertiesAsync(leaseChild2), Times.Once);

            Mock.Get(partitionSupervisorFactory)
                .Verify(f => f.Create(leaseChild2), Times.Once);
        }

        [Fact]
        public async Task Controller_ShouldDeleteParentLease_IfChildLeaseAcquireThrows()
        {
            Mock.Get(synchronizer)
                .Setup(s => s.SplitPartitionAsync(lease))
                .ReturnsAsync(new[] { leaseChild, leaseChild2 });

            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(leaseChild))
                .Returns<ILease>(l => new PartitionSupervisor(l, MockObserver(), MockPartitionProcessor(), MockRenewer()));

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(leaseChild))
                .ReturnsAsync(leaseChild);

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(leaseChild2))
                .Throws(new LeaseLostException());

            Mock.Get(leaseManager)
                .Setup(manager => manager.DeleteAsync(lease))
                .Returns(Task.FromResult(false));

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);
        }

        public Task InitializeAsync()
        {
            return Task.FromResult(false);
        }

        public async Task DisposeAsync()
        {
            await sut.ShutdownAsync().ConfigureAwait(false);

            Mock.Get(leaseManager)
                .VerifyAll();

            Mock.Get(partitionSupervisorFactory)
                .VerifyAll();

            Mock.Get(synchronizer)
                .VerifyAll();
        }

        private static IPartitionProcessor MockPartitionProcessor()
        {
            var mock = new Mock<IPartitionProcessor>();
            mock
                .Setup(p => p.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(token => Task.Delay(TimeSpan.FromHours(1), token));
            return mock.Object;
        }

        private static ILeaseRenewer MockRenewer()
        {
            var mock = new Mock<ILeaseRenewer>();
            mock
                .Setup(renewer => renewer.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(token => Task.Delay(TimeSpan.FromMinutes(1), token));
            return mock.Object;
        }

        private static IChangeFeedObserver MockObserver()
        {
            var mock = new Mock<IChangeFeedObserver>();
            mock.Setup(observer => observer.OpenAsync(It.IsAny<ChangeFeedObserverContext>()))
                .Returns(Task.FromResult(false));
            return mock.Object;
        }
    }
}