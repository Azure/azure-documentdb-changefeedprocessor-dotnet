//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class PartitionSupervisorTests : IDisposable
    {
        private readonly ILease lease;
        private readonly ILeaseRenewer leaseRenewer;
        private readonly IPartitionProcessor partitionProcessor;
        private readonly IChangeFeedObserver observer;
        private readonly CancellationTokenSource shutdownToken = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        private readonly PartitionSupervisor sut;

        public PartitionSupervisorTests()
        {
            lease = Mock.Of<ILease>();
            Mock.Get(lease)
                .Setup(l => l.PartitionId)
                .Returns("partitionId");

            leaseRenewer = Mock.Of<ILeaseRenewer>();
            partitionProcessor = Mock.Of<IPartitionProcessor>();
            observer = Mock.Of<IChangeFeedObserver>();

            sut = new PartitionSupervisor(lease, observer, partitionProcessor, leaseRenewer);
        }

        [Fact]
        public async Task RunObserver_ShouldCancelTasks_WhenTokenCanceled()
        {
            Task renewerTask = Task.FromResult(false);
            Mock.Get(leaseRenewer)
                .Setup(renewer => renewer.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(token => renewerTask = Task.Delay(TimeSpan.FromMinutes(1), token));

            Task processorTask = Task.FromResult(false);
            Mock.Get(partitionProcessor)
                .Setup(processor => processor.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(token => processorTask = Task.Delay(TimeSpan.FromMinutes(1), token));

            Task supervisorTask = sut.RunAsync(shutdownToken.Token);

            Task delay = Task.Delay(TimeSpan.FromMilliseconds(100));
            Task finished = await Task.WhenAny(supervisorTask, delay).ConfigureAwait(false);
            Assert.Equal(delay, finished);

            shutdownToken.Cancel();
            await supervisorTask.ConfigureAwait(false);

            Assert.True(renewerTask.IsCanceled);
            Assert.True(processorTask.IsCanceled);
            Mock.Get(partitionProcessor)
                .Verify(processor => processor.RunAsync(It.IsAny<CancellationToken>()), Times.Once);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                    .CloseAsync(It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == lease.PartitionId),
                        ChangeFeedObserverCloseReason.Shutdown));
        }

        [Fact]
        public async Task RunObserver_ShouldCancelProcessor_IfRenewerFailed()
        {
            Task processorTask = Task.FromResult(false);
            Mock.Get(leaseRenewer)
                .Setup(renewer => renewer.RunAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new LeaseLostException());

            Mock.Get(partitionProcessor)
                .Setup(processor => processor.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(token => processorTask = Task.Delay(TimeSpan.FromMinutes(1), token));

            Exception exception = await Record.ExceptionAsync(() => sut.RunAsync(shutdownToken.Token)).ConfigureAwait(false);
            Assert.IsType<LeaseLostException>(exception);
            Assert.True(processorTask.IsCanceled);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                    .CloseAsync(It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == lease.PartitionId),
                        ChangeFeedObserverCloseReason.LeaseLost));
        }

        [Fact]
        public async Task RunObserver_ShouldCancelRenewer_IfProcessorFailed()
        {
            Task renewerTask = Task.FromResult(false);
            Mock.Get(leaseRenewer)
                .Setup(renewer => renewer.RunAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(token => renewerTask = Task.Delay(TimeSpan.FromMinutes(1), token));

            Mock.Get(partitionProcessor)
                .Setup(processor => processor.RunAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new Exception("processorException"));

            Exception exception = await Record.ExceptionAsync(() => sut.RunAsync(shutdownToken.Token)).ConfigureAwait(false);
            Assert.IsType<Exception>(exception);
            Assert.Equal("processorException", exception.Message);
            Assert.True(renewerTask.IsCanceled);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                    .CloseAsync(It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == lease.PartitionId),
                        ChangeFeedObserverCloseReason.Unknown));
        }

        [Fact]
        public async Task RunObserver_ShouldCloseWithObserverError_IfObserverFailed()
        {
            Mock.Get(partitionProcessor)
                .Setup(processor => processor.RunAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ObserverException(new Exception()));

            Exception exception = await Record.ExceptionAsync(() => sut.RunAsync(shutdownToken.Token)).ConfigureAwait(false);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                    .CloseAsync(It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == lease.PartitionId),
                        ChangeFeedObserverCloseReason.ObserverError));
        }

        [Fact]
        public async Task RunObserver_ShouldPassPartitionToObserver_WhenExecuted()
        {
            Mock.Get(observer)
                .Setup(feedObserver => feedObserver.ProcessChangesAsync(It.IsAny<ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Callback(() => shutdownToken.Cancel());

            await sut.RunAsync(shutdownToken.Token).ConfigureAwait(false);
            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                    .OpenAsync(It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == lease.PartitionId)));
        }

        [Fact]
        public void Dispose_ShouldWork_WithoutRun()
        {
            Exception exception = Record.Exception(() => sut.Dispose());
            Assert.Null(exception);
        }

        public void Dispose()
        {
            sut.Dispose();
            shutdownToken.Dispose();
        }
    }
}