//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.FeedProcessor
{
    public class AutoCheckPointTests
    {
        private readonly IChangeFeedObserver changeFeedObserver;
        private readonly IChangeFeedObserverContext observerContext;
        private readonly CheckpointFrequency checkpointFrequency;
        private readonly AutoCheckpointer sut;
        private readonly IReadOnlyList<Document> documents;

        public AutoCheckPointTests()
        {
            changeFeedObserver = Mock.Of<IChangeFeedObserver>();
            observerContext = Mock.Of<IChangeFeedObserverContext>();

            checkpointFrequency = new CheckpointFrequency();
            sut = new AutoCheckpointer(checkpointFrequency, changeFeedObserver);

            documents = Mock.Of<IReadOnlyList<Document>>();
        }

        [Fact]
        public async Task OpenAsync_WhenCalled_ShouldOpenObserver()
        {
            await sut.OpenAsync(observerContext);

            Mock.Get(changeFeedObserver)
                .Verify(observer => observer.OpenAsync(observerContext), Times.Once);
        }

        [Fact]
        public async Task CloseAsync_WhenCalled_ShouldCloseObserver()
        {
            await sut.CloseAsync(observerContext, ChangeFeedObserverCloseReason.ResourceGone);

            Mock.Get(changeFeedObserver)
                .Verify(observer => observer.CloseAsync(observerContext, ChangeFeedObserverCloseReason.ResourceGone), Times.Once);
        }

        [Fact]
        public async Task ProcessChanges_WhenCalled_ShouldPassTheBatch()
        {
            await sut.ProcessChangesAsync(observerContext, documents);

            Mock.Get(changeFeedObserver)
                .Verify(observer => observer.ProcessChangesAsync(observerContext, documents), Times.Once);
        }

        [Fact]
        public async Task ProcessChanges_WhenCheckpointThrows_ShouldThrow()
        {
            checkpointFrequency.TimeInterval = TimeSpan.Zero;

            Mock.Get(observerContext)
                .Setup(context => context.CheckpointAsync())
                .ThrowsAsync(new LeaseLostException());

            Exception ex = await Record.ExceptionAsync(() => sut.ProcessChangesAsync(observerContext, documents));
            Assert.IsType<LeaseLostException>(ex);
        }

        [Fact]
        public async Task ProcessChanges_WhenPeriodPass_ShouldCheckpoint()
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            checkpointFrequency.TimeInterval = TimeSpan.FromHours(1);
            await sut.ProcessChangesAsync(observerContext, documents);
            Mock.Get(observerContext)
                .Verify(context => context.CheckpointAsync(), Times.Never);

            await Task.Delay(TimeSpan.FromSeconds(1));

            checkpointFrequency.TimeInterval = stopwatch.Elapsed;
            await sut.ProcessChangesAsync(observerContext, documents);
            Mock.Get(observerContext)
                .Verify(context => context.CheckpointAsync(), Times.Once);
        }

        [Fact]
        public async Task ProcessChanges_WithDocTrigger_ShouldCheckpointWhenAbove()
        {
            Mock.Get(documents)
                .Setup(list => list.Count)
                .Returns(1);

            checkpointFrequency.ProcessedDocumentCount = 2;

            await sut.ProcessChangesAsync(observerContext, documents);
            Mock.Get(observerContext)
                .Verify(context => context.CheckpointAsync(), Times.Never);

            await sut.ProcessChangesAsync(observerContext, documents);
            Mock.Get(observerContext)
                .Verify(context => context.CheckpointAsync(), Times.Once);

            await sut.ProcessChangesAsync(observerContext, documents);
            Mock.Get(observerContext)
                .Verify(context => context.CheckpointAsync(), Times.Once);

            await sut.ProcessChangesAsync(observerContext, documents);
            Mock.Get(observerContext)
                .Verify(context => context.CheckpointAsync(), Times.Exactly(2));
        }
    }
}