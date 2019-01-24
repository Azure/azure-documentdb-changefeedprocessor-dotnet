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
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
    using Microsoft.Azure.Documents.Client;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class PartitionProcessorTests
    {
        private readonly ProcessorSettings processorSettings;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly PartitionProcessor sut;
        private readonly IChangeFeedDocumentClient docClient;
        private readonly IChangeFeedDocumentQuery<Document> documentQuery;
        private readonly IFeedResponse<Document> feedResponse;
        private readonly IChangeFeedObserver observer;
        private readonly List<Document> documents;

        public PartitionProcessorTests()
        {
            processorSettings = new ProcessorSettings
            {
                CollectionSelfLink = "selfLink",
                FeedPollDelay = TimeSpan.FromMilliseconds(16),
                MaxItemCount = 5,
                PartitionKeyRangeId = "keyRangeId",
                StartContinuation = "initialToken"
            };

            var document = new Document();
            documents = new List<Document> { document };

            feedResponse = Mock.Of<IFeedResponse<Document>>();
            Mock.Get(feedResponse)
                .Setup(response => response.Count)
                .Returns(documents.Count);
            Mock.Get(feedResponse)
                .Setup(response => response.ResponseContinuation)
                .Returns("token");
            Mock.Get(feedResponse)
                .Setup(response => response.GetEnumerator())
                .Returns(documents.GetEnumerator());

            documentQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>();
            Mock.Get(documentQuery)
                .Setup(query => query.HasMoreResults)
                .Returns(false);

            Mock.Get(documentQuery)
                .Setup(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .ReturnsAsync(() => feedResponse)
                .Callback(() => cancellationTokenSource.Cancel());

            docClient = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(docClient)
                .Setup(ex => ex.CreateDocumentChangeFeedQuery(processorSettings.CollectionSelfLink, It.IsAny<ChangeFeedOptions>()))
                .Returns(documentQuery);

            observer = Mock.Of<IChangeFeedObserver>();
            var checkPointer = new Mock<IPartitionCheckpointer>();
            sut = new PartitionProcessor(new FeedProcessing.ObserverExceptionWrappingChangeFeedObserverDecorator(observer), docClient, processorSettings, checkPointer.Object);
        }

        [Fact]
        public async Task Run_ShouldThrowException_IfCanceled()
        {
            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));
        }

        [Fact]
        public async Task Run_ShouldPassDocumentsToObserver_IfDocumentExists()
        {
            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<IChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents)), 
                            It.IsAny<CancellationToken>()),
                    Times.Once);
        }

        [Fact]
        public async Task Run_ShouldPassFeedOptionsToQuery_OnCreation()
        {
            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));

            Mock.Get(docClient)
                .Verify(d => d.CreateDocumentChangeFeedQuery(
                        It.Is<string>(s => s == processorSettings.CollectionSelfLink),
                        It.Is<ChangeFeedOptions>(options =>
                            options.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId &&
                            options.RequestContinuation == processorSettings.StartContinuation)),
                    Times.Once);
        }

        [Fact]
        public async Task Run_ShouldPassTheTokenOnce_WhenCanceled()
        {
            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));

            Mock.Get(documentQuery)
                .Verify(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)), Times.Once);
        }

        [Fact]
        public async Task Run_ShouldContinue_IfDocDBThrowsCanceled()
        {
            Mock.Get(documentQuery)
                .Reset();

            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Throws(new TaskCanceledException("canceled in test"))
                .ReturnsAsync(feedResponse);

            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask)
                .Callback(cancellationTokenSource.Cancel);

            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<IChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents)),
                            It.IsAny<CancellationToken>()),
                    Times.Once);
        }

        [Fact]
        public async Task Run_ShouldRetryQuery_IfDocDBThrowsCanceled()
        {
            Mock.Get(documentQuery)
                .Reset();

            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Throws(new TaskCanceledException("canceled in test"))
                .ReturnsAsync(feedResponse);

            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask)
                .Callback(cancellationTokenSource.Cancel);

            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));

            Mock.Get(documentQuery)
                .Verify(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)), Times.Exactly(2));

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<IChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents)),
                            It.IsAny<CancellationToken>()),
                    Times.Once);
        }

        [Fact]
        public async Task Run_ShouldThrow_IfObserverThrows()
        {
            Mock.Get(documentQuery)
                .Reset();

            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .ReturnsAsync(feedResponse);

            Mock.Get(observer)
                .SetupSequence(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Throws(new CustomException())
                .Returns(Task.CompletedTask);

            Exception exception = await Record.ExceptionAsync(() => sut.RunAsync(cancellationTokenSource.Token));
            Assert.IsAssignableFrom<ObserverException>(exception);
            Assert.IsAssignableFrom<CustomException>(exception.InnerException);

            Mock.Get(documentQuery)
                .Verify(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)), Times.Once);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<IChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents)),
                            It.IsAny<CancellationToken>()),
                    Times.Once);
        }

        [Fact]
        public async Task Run_ShouldThrow_IfObserverThrowsDocumentClientException()
        {
            // If the user code throws a DCE, we should bubble it up to stop the Observer and not treat it as a DCE from the Feed Query

            Mock.Get(documentQuery)
                .Reset();

            Mock.Get(documentQuery)
                .Setup(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .ReturnsAsync(feedResponse)
                .Callback(() => cancellationTokenSource.Cancel());

            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Throws(DocumentExceptionHelpers.CreateRequestRateTooLargeException());

            Exception exception = await Record.ExceptionAsync(() => sut.RunAsync(cancellationTokenSource.Token));
            Assert.IsAssignableFrom<ObserverException>(exception);
            Assert.IsAssignableFrom<DocumentClientException>(exception.InnerException);

            Mock.Get(documentQuery)
                .Verify(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)), Times.Once);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<IChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents)),
                            It.IsAny<CancellationToken>()),
                    Times.Once);
        }

        /// <summary>
        /// (1) Read normal feed
        /// (2) Get 400 with 
        /// (3) Continue read normal feed
        /// </summary>
        [Fact]
        public async Task Run_ShouldDecreaseMaxItemCountWhenNeeded()
        {
            var documents2 = new List<Document> { new Document(), new Document() };

            var feedResponse2 = Mock.Of<IFeedResponse<Document>>();
            Mock.Get(feedResponse2)
                .Setup(response => response.Count)
                .Returns(documents2.Count);
            Mock.Get(feedResponse2)
                .Setup(response => response.ResponseContinuation)
                .Returns("token2");
            Mock.Get(feedResponse2)
                .Setup(response => response.GetEnumerator())
                .Returns(documents2.GetEnumerator());

            var documents3 = new List<Document> { new Document(), new Document(), new Document() };

            var feedResponse3 = Mock.Of<IFeedResponse<Document>>();
            Mock.Get(feedResponse3)
                .Setup(response => response.Count)
                .Returns(documents3.Count);
            Mock.Get(feedResponse3)
                .Setup(response => response.ResponseContinuation)
                .Returns("token3");
            Mock.Get(feedResponse3)
                .Setup(response => response.GetEnumerator())
                .Returns(documents3.GetEnumerator());

            Mock.Get(documentQuery)
                .Reset();
            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .ReturnsAsync(feedResponse)     // 1st call is OK.
                .Throws(DocumentExceptionHelpers.CreateException("Microsoft.Azure.Documents.BadRequestException", 1, "Reduce page size and try again."))
                .Throws(DocumentExceptionHelpers.CreateException("Microsoft.Azure.Documents.BadRequestException", 1, "Reduce page size and try again."))
                .ReturnsAsync(feedResponse2)    // Call with maxItemCount = 1.
                .ReturnsAsync(feedResponse3);   // After restoring query to take default item count.

            // (accumulator += context.FeedResponse.ResponseContinuation + ".") != null && 
            string accumulator = string.Empty;
            int observerCallCount = 0;
            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<IChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(),It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask)
                .Callback<IChangeFeedObserverContext, IReadOnlyList<Document>, CancellationToken>((context, docs, token) => 
                {
                    accumulator += context.FeedResponse.ResponseContinuation + ".";
                    if (++observerCallCount == 3) cancellationTokenSource.Cancel();
                });

            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));

            Mock.Get(documentQuery)
                .Verify(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)), Times.Exactly(5));

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(
                                list => list.Count == 1 ? list.SequenceEqual(documents) :
                                        list.Count == 2 ? list.SequenceEqual(documents2) :
                                        list.SequenceEqual(documents3)),
                            It.IsAny<CancellationToken>()),
                    Times.Exactly(3));

            Assert.Equal("token.token2.token3.", accumulator);
        }

        private class CustomException : Exception
        {
        }
    }
}