//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.FeedProcessor
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class ObserverExceptionWrappingChangeFeedObserverDecoratorTests
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly IChangeFeedObserver observer;
        private readonly IChangeFeedObserverContext changeFeedObserverContext;
        private readonly FeedProcessing.ObserverExceptionWrappingChangeFeedObserverDecorator observerWrapper;
        private readonly List<Document> documents;

        public ObserverExceptionWrappingChangeFeedObserverDecoratorTests()
        {
            this.observer = Mock.Of<IChangeFeedObserver>();
            this.changeFeedObserverContext = Mock.Of<IChangeFeedObserverContext>();
            this.observerWrapper = new FeedProcessing.ObserverExceptionWrappingChangeFeedObserverDecorator(observer);

            var document = new Document();
            documents = new List<Document> { document };
        }

        [Fact]
        public async Task OpenAsync_ShouldCallOpenAsync()
        {
            await observerWrapper.OpenAsync(this.changeFeedObserverContext);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .OpenAsync(It.IsAny<IChangeFeedObserverContext>()),
                    Times.Once);
        }

        [Fact]
        public async Task CloseAsync_ShouldCallCloseAsync()
        {
            await observerWrapper.CloseAsync(this.changeFeedObserverContext, ChangeFeedObserverCloseReason.Shutdown);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .CloseAsync(It.IsAny<IChangeFeedObserverContext>(),
                        It.Is<ChangeFeedObserverCloseReason>(reason => reason == ChangeFeedObserverCloseReason.Shutdown)),
                    Times.Once);
        }

        [Fact]
        public async Task ProcessChangesAsync_ShouldPassDocumentsToProcessChangesAsync()
        {
            await observerWrapper.ProcessChangesAsync(this.changeFeedObserverContext, this.documents, cancellationTokenSource.Token);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents)),
                            It.IsAny<CancellationToken>()
                        ),
                    Times.Once);
        }

        [Fact]
        public async Task ProcessChangesAsync_ShouldThrow_IfObserverThrows()
        {
            Mock.Get(observer)
                .SetupSequence(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Throws(new Exception());

            Exception exception = await Record.ExceptionAsync(() => observerWrapper.ProcessChangesAsync(this.changeFeedObserverContext, this.documents, cancellationTokenSource.Token));
            Assert.IsAssignableFrom<ObserverException>(exception);
            Assert.IsAssignableFrom<Exception>(exception.InnerException);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents)),
                            It.IsAny<CancellationToken>()
                        ),
                    Times.Once);
        }

        [Fact]
        public async Task ProcessChangesAsync_ShouldThrow_IfObserverThrowsDocumentClientException()
        {
            Mock.Get(observer)
                .SetupSequence(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Throws(DocumentExceptionHelpers.CreateRequestRateTooLargeException());

            Exception exception = await Record.ExceptionAsync(() => observerWrapper.ProcessChangesAsync(this.changeFeedObserverContext, this.documents, cancellationTokenSource.Token));
            Assert.IsAssignableFrom<ObserverException>(exception);
            Assert.IsAssignableFrom<DocumentClientException>(exception.InnerException);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents)),
                            It.IsAny<CancellationToken>()
                        ),
                    Times.Once);
        }
    }
}
