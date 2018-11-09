//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class PartitionControllerTests : IAsyncLifetime
    {
        private readonly ILease lease;
        private readonly ILeaseManager leaseManager;
        private readonly IPartitionProcessor partitionProcessor;
        private readonly ILeaseRenewer leaseRenewer;
        private readonly IChangeFeedObserver observer;
        private readonly IPartitionSynchronizer synchronizer;
        private readonly PartitionController sut;
        private readonly IPartitionSupervisorFactory partitionSupervisorFactory;

        public PartitionControllerTests()
        {
            lease = Mock.Of<ILease>();
            Mock.Get(lease)
                .Setup(l => l.PartitionId)
                .Returns("partitionId");

            partitionProcessor = MockPartitionProcessor();
            leaseRenewer = MockRenewer();
            observer = MockObserver();
            partitionSupervisorFactory = Mock.Of<IPartitionSupervisorFactory>(f => f.Create(lease) == new PartitionSupervisor(lease, observer, partitionProcessor, leaseRenewer));

            leaseManager = Mock.Of<ILeaseManager>();
            Mock.Get(leaseManager).Reset(); // Reset implicit/by default setup of properties.
            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(lease))
                .ReturnsAsync(lease);
            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.CompletedTask);
            var leaseContainer = Mock.Of<ILeaseContainer>();

            synchronizer = Mock.Of<IPartitionSynchronizer>();
            sut = new PartitionController(leaseContainer, leaseManager, partitionSupervisorFactory, synchronizer);
        }

        [Fact]
        public async Task AddLease_ShouldAcquireLease_WhenCalled()
        {
            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            Mock.Get(leaseManager)
                .Verify(manager => manager.AcquireAsync(lease), Times.Once);
        }

        [Fact]
        public async Task AddLease_ShouldRunObserver_WhenCalled()
        {
            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            Mock.Get(partitionProcessor)
                .Verify(p => p.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task AddLease_ShouldntReleaseLease_WhenCalled()
        {
            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            Mock.Get(partitionProcessor)
                .Verify(p => p.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task AddLease_ShouldIgnorePartitionObserving_IfDuplicateLease()
        {
            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            IPartitionProcessor processorDuplicate = MockPartitionProcessor();
            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(lease))
                .Returns(new PartitionSupervisor(lease, observer, processorDuplicate, leaseRenewer));

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            Mock.Get(leaseManager)
                .Verify(manager => manager.AcquireAsync(lease), Times.Once);

            Mock.Get(leaseManager)
                .Verify(manager => manager.UpdatePropertiesAsync(lease), Times.Once);

            Mock.Get(leaseManager)
                .Verify(manager => manager.ReleaseAsync(It.IsAny<ILease>()), Times.Never);

            Mock.Get(partitionProcessor)
                .Verify(p => p.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
            Mock.Get(processorDuplicate)
                .Verify(p => p.RunAsync(It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task AddLease_ShouldNotRelease_IfUpdateLeasePropertiesThrows()
        {
            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);

            Mock.Get(partitionProcessor)
                .Reset();

            Mock.Get(leaseManager)
                .Reset();

            Mock.Get(leaseManager)
                .Setup(manager => manager.UpdatePropertiesAsync(lease))
                .Throws(new NullReferenceException());

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.CompletedTask);

            Exception actualException = await Record.ExceptionAsync(() => sut.AddOrUpdateLeaseAsync(lease)).ConfigureAwait(false);
            Assert.IsType<NullReferenceException>(actualException);

            Mock.Get(leaseManager)
                .Verify(manager => manager.ReleaseAsync(It.IsAny<ILease>()), Times.Never);
        }

        [Fact]
        public async Task AddLease_ShouldAcquireLease_IfSecondLeaseAdded()
        {
            var lease2 = Mock.Of<ILease>();
            Mock.Get(lease2)
                .Setup(l => l.PartitionId)
                .Returns("partitionId2");

            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(lease2))
                .Returns(new PartitionSupervisor(lease2, observer, MockPartitionProcessor(), leaseRenewer));

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);
            await sut.AddOrUpdateLeaseAsync(lease2).ConfigureAwait(false);

            Mock.Get(leaseManager)
                .Verify(manager => manager.AcquireAsync(lease2), Times.Once);
        }

        [Fact]
        public async Task AddLease_ShouldRunObserver_IfSecondAdded()
        {
            var lease2 = Mock.Of<ILease>();
            Mock.Get(lease2)
                .Setup(l => l.PartitionId)
                .Returns("partitionId2");

            IPartitionProcessor partitionProcessor2 = MockPartitionProcessor();
            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(lease2))
                .Returns(new PartitionSupervisor(lease2, observer, partitionProcessor2, leaseRenewer));

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);
            await sut.AddOrUpdateLeaseAsync(lease2).ConfigureAwait(false);

            Mock.Get(partitionProcessor2)
                .Verify(p => p.RunAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task Shutdown_ShouldWork_WithoutLeases()
        {
            Mock.Get(leaseManager)
                .Reset();

            Mock.Get(partitionProcessor)
                .Reset();

            await sut.ShutdownAsync().ConfigureAwait(false);

            Mock.Get(leaseManager)
                .Verify(manager => manager.ReleaseAsync(It.IsAny<ILease>()), Times.Never);
        }

        [Fact]
        public async Task Controller_ShouldReleasesLease_IfObserverExits()
        {
            Mock.Get(partitionProcessor)
                .Reset();

            Mock.Get(partitionSupervisorFactory)
                .Setup(f => f.Create(lease))
                .Returns(new PartitionSupervisor(lease, observer, partitionProcessor, leaseRenewer));

            await sut.AddOrUpdateLeaseAsync(lease).ConfigureAwait(false);
            await Task.Delay(TimeSpan.FromMilliseconds(100)).ConfigureAwait(false);

            Mock.Get(leaseManager)
                .Verify(manager => manager.ReleaseAsync(It.IsAny<ILease>()), Times.Once);
        }

        [Fact]
        public async Task AddLease_ShouldFail_IfLeaseAcquireThrows()
        {
            Mock.Get(partitionProcessor)
                .Reset();

            Mock.Get(leaseManager)
                .Reset();

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(lease))
                .Throws(new NullReferenceException());

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.CompletedTask);

            Exception actualException = await Record.ExceptionAsync(() => sut.AddOrUpdateLeaseAsync(lease)).ConfigureAwait(false);
            Assert.IsType<NullReferenceException>(actualException);
        }

        [Fact]
        public async Task AddLease_ShouldReleaseLease_IfLeaseAcquireThrows()
        {
            Mock.Get(partitionProcessor)
                .Reset();

            Mock.Get(leaseManager)
                .Reset();

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(lease))
                .Throws(new NullReferenceException());

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.CompletedTask);

            Exception actualException = await Record.ExceptionAsync(() => sut.AddOrUpdateLeaseAsync(lease)).ConfigureAwait(false);
            Assert.IsType<NullReferenceException>(actualException);

            Mock.Get(leaseManager)
                .Verify(manager => manager.ReleaseAsync(It.IsAny<ILease>()), Times.Once);
        }

        [Fact]
        public async Task AddLease_ShouldntRunObserver_IfLeaseAcquireThrows()
        {
            Mock.Get(partitionProcessor)
                .Reset();

            Mock.Get(leaseManager)
                .Reset();

            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(lease))
                .Throws(new NullReferenceException());

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.CompletedTask);

            Exception actualException = await Record.ExceptionAsync(() => sut.AddOrUpdateLeaseAsync(lease)).ConfigureAwait(false);
            Assert.IsType<NullReferenceException>(actualException);

            Mock.Get(partitionProcessor)
                .Verify(processor => processor.RunAsync(It.IsAny<CancellationToken>()), Times.Never);
        }

        public Task InitializeAsync()
        {
            return Task.FromResult(false);
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
            return mock.Object;
        }

        public async Task DisposeAsync()
        {
            await sut.ShutdownAsync().ConfigureAwait(false);

            Mock.Get(leaseManager)
                .VerifyAll();

            Mock.Get(partitionProcessor)
                .VerifyAll();

            Mock.Get(synchronizer)
                .VerifyAll();
        }
    }
}