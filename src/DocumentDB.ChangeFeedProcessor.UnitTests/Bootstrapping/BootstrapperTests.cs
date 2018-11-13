//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping;
using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Bootstrapping
{
    [Trait("Category", "Gated")]
    public class BootstrapperTests
    {
        private readonly TimeSpan sleepTime = TimeSpan.FromMilliseconds(1);
        private readonly TimeSpan lockTime = TimeSpan.FromMilliseconds(100);

        [Fact]
        public async Task InitializeAsync_ShouldNotCreateMissingLeases_IfStoreIsInitialized()
        {
            var synchronizer = Mock.Of<IPartitionSynchronizer>();
            var leaseStore = Mock.Of<ILeaseStore>();
            Mock.Get(leaseStore)
                .Setup(store => store.IsInitializedAsync())
                .ReturnsAsync(true);

            var bootstrapper = new Bootstrapper(synchronizer, leaseStore, lockTime, sleepTime);
            await bootstrapper.InitializeAsync().ConfigureAwait(false);

            Mock.Get(synchronizer)
                .Verify(s => s.CreateMissingLeasesAsync(), Times.Never);
        }

        [Fact]
        public async Task InitializeAsync_CreatesMissingLeasesAndMarkStore_IfNoStore()
        {
            var synchronizer = Mock.Of<IPartitionSynchronizer>();
            Mock.Get(synchronizer)
                .Setup(s => s.CreateMissingLeasesAsync())
                .Returns(Task.CompletedTask);

            var leaseStore = Mock.Of<ILeaseStore>();
            Mock.Get(leaseStore)
                .Setup(store => store.IsInitializedAsync())
                .ReturnsAsync(false);

            Mock.Get(leaseStore)
                .Setup(store => store.AcquireInitializationLockAsync(lockTime))
                .ReturnsAsync(true);

            Mock.Get(leaseStore)
                .Setup(store => store.MarkInitializedAsync())
                .Returns(Task.CompletedTask);

            var bootstrapper = new Bootstrapper(synchronizer, leaseStore, lockTime, sleepTime);
            await bootstrapper.InitializeAsync().ConfigureAwait(false);

            Mock.Get(synchronizer)
                .Verify(s => s.CreateMissingLeasesAsync(), Times.Once);

            Mock.Get(leaseStore)
                .Verify(l => l.MarkInitializedAsync(), Times.Once);
        }

        [Fact]
        public async Task InitializeAsync_WaitForInit_IfInitializingStore()
        {
            var synchronizer = Mock.Of<IPartitionSynchronizer>();
            var leaseStore = Mock.Of<ILeaseStore>();
            Mock.Get(leaseStore)
                .SetupSequence(store => store.IsInitializedAsync())
                .ReturnsAsync(false)
                .ReturnsAsync(false)
                .ReturnsAsync(true);

            Mock.Get(leaseStore)
                .SetupSequence(store => store.AcquireInitializationLockAsync(lockTime))
                .ReturnsAsync(false)
                .ReturnsAsync(false);

            var bootstrapper = new Bootstrapper(synchronizer, leaseStore, lockTime, sleepTime);
            await bootstrapper.InitializeAsync().ConfigureAwait(false);

            Mock.Get(leaseStore)
                .Verify(store => store.IsInitializedAsync(), Times.Exactly(3));

            Mock.Get(leaseStore)
                .Verify(store => store.AcquireInitializationLockAsync(lockTime), Times.Exactly(2));

            Mock.Get(synchronizer)
                .Verify(s => s.CreateMissingLeasesAsync(), Times.Never);

            Mock.Get(leaseStore)
                .Verify(l => l.MarkInitializedAsync(), Times.Never);
        }

        [Fact]
        public async Task InitializeAsync_ShouldLeaveUninitedStore_IfCantCreateLeases()
        {
            var synchronizer = Mock.Of<IPartitionSynchronizer>();
            Mock.Get(synchronizer)
                .Setup(s => s.CreateMissingLeasesAsync())
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException());

            var leaseStore = Mock.Of<ILeaseStore>();
            Mock.Get(leaseStore)
                .Setup(store => store.IsInitializedAsync())
                .ReturnsAsync(false);

            Mock.Get(leaseStore)
                .Setup(store => store.AcquireInitializationLockAsync(lockTime))
                .ReturnsAsync(true);

            var bootstrapper = new Bootstrapper(synchronizer, leaseStore, lockTime, sleepTime);
            Exception exception = await Record.ExceptionAsync(async () => await bootstrapper.InitializeAsync());
            Assert.IsAssignableFrom<DocumentClientException>(exception);

            Mock.Get(leaseStore)
                .Verify(l => l.MarkInitializedAsync(), Times.Never);
        }

        [Fact]
        public async Task InitializeAsync_ShouldReleaseLock_OnSuccess()
        {
            var synchronizer = Mock.Of<IPartitionSynchronizer>();
            Mock.Get(synchronizer)
                .Setup(s => s.CreateMissingLeasesAsync())
                .Returns(Task.CompletedTask);

            var leaseStore = Mock.Of<ILeaseStore>();
            Mock.Get(leaseStore)
                .Setup(store => store.IsInitializedAsync())
                .ReturnsAsync(false);
            Mock.Get(leaseStore)
                .Setup(store => store.AcquireInitializationLockAsync(lockTime))
                .ReturnsAsync(true);
            Mock.Get(leaseStore)
                .Setup(store => store.MarkInitializedAsync()).
                Returns(Task.CompletedTask);
            Mock.Get(leaseStore)
                .Setup(store => store.ReleaseInitializationLockAsync())
                .ReturnsAsync(true);

            var bootstrapper = new Bootstrapper(synchronizer, leaseStore, lockTime, sleepTime);
            await bootstrapper.InitializeAsync();

            Mock.Get(leaseStore).VerifyAll();
        }

        [Fact]
        public async Task InitializeAsync_ShouldReleaseLock_OnException()
        {
            var synchronizer = Mock.Of<IPartitionSynchronizer>();
            Mock.Get(synchronizer)
                .Setup(s => s.CreateMissingLeasesAsync())
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException());

            var leaseStore = Mock.Of<ILeaseStore>();
            Mock.Get(leaseStore)
                .Setup(store => store.IsInitializedAsync())
                .ReturnsAsync(false);
            Mock.Get(leaseStore)
                .Setup(store => store.AcquireInitializationLockAsync(lockTime))
                .ReturnsAsync(true);
            Mock.Get(leaseStore)
                .Setup(store => store.ReleaseInitializationLockAsync())
                .ReturnsAsync(true);

            var bootstrapper = new Bootstrapper(synchronizer, leaseStore, lockTime, sleepTime);
            Exception exception = await Record.ExceptionAsync(async () => await bootstrapper.InitializeAsync());
            Assert.IsAssignableFrom<DocumentClientException>(exception);

            Mock.Get(leaseStore).VerifyAll();
        }
    }
}