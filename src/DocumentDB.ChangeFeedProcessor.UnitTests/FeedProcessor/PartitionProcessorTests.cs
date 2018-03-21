//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Processing;
using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
using Microsoft.Azure.Documents.Client;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.FeedProcessor
{
    [Trait("Category", "Gated")]
    public class PartitionProcessorTests
    {
        private readonly ProcessorSettings processorSettings;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly PartitionProcessor sut;
        private readonly IChangeFeedDocumentClient docClient;
        private readonly IChangeFeedDocumentQuery<Document> documentQuery;
        private readonly IFeedResponse<Document> feedResponse;
        private readonly Processing.IChangeFeedObserver observer;
        private readonly List<Document> documents;

        public PartitionProcessorTests()
        {
            processorSettings = new ProcessorSettings
            {
                CollectionSelfLink = "selfLink",
                FeedPollDelay = TimeSpan.FromMilliseconds(16),
                MaxItemCount = 5,
                PartitionKeyRangeId = "keyRangeId",
                RequestContinuation = "initialToken"
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

            observer = Mock.Of<Processing.IChangeFeedObserver>();
            var checkPointer = new Mock<IPartitionCheckpointer>();
            sut = new PartitionProcessor(observer, docClient, processorSettings, checkPointer.Object);
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
                            It.Is<Processing.ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
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
                            options.RequestContinuation == processorSettings.RequestContinuation)),
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
                    .ProcessChangesAsync(It.IsAny<Processing.ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(false))
                .Callback(cancellationTokenSource.Cancel);

            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<Processing.ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
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
                    .ProcessChangesAsync(It.IsAny<Processing.ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(false))
                .Callback(cancellationTokenSource.Cancel);

            await Assert.ThrowsAsync<TaskCanceledException>(() => sut.RunAsync(cancellationTokenSource.Token));

            Mock.Get(documentQuery)
                .Verify(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)), Times.Exactly(2));

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<Processing.ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
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
                    .ProcessChangesAsync(It.IsAny<Processing.ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(),It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(false))
                .Callback<Processing.ChangeFeedObserverContext, IReadOnlyList<Document>>((context, docs) => 
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
                            It.Is<Processing.ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(
                                list => list.Count == 1 ? list.SequenceEqual(documents) :
                                        list.Count == 2 ? list.SequenceEqual(documents2) :
                                        list.SequenceEqual(documents3)),
                            It.IsAny<CancellationToken>()),
                    Times.Exactly(3));

            Assert.Equal("token.token2.token3.", accumulator);
        }
    }
}