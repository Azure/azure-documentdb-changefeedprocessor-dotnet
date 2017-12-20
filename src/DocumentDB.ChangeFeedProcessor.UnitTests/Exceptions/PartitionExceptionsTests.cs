//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
using Microsoft.Azure.Documents.Client;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Exceptions
{
    public class PartitionExceptionsTests
    {
        private readonly ProcessorSettings processorSettings;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly PartitionProcessor partitionProcessor;
        private readonly IDocumentClientEx docClient;
        private readonly IDocumentQueryEx<Document> documentQuery;
        private readonly IFeedResponse<Document> feedResponse;
        private readonly IChangeFeedObserver observer;
        private readonly List<Document> documents;

        public PartitionExceptionsTests()
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

            documentQuery = Mock.Of<IDocumentQueryEx<Document>>();
            Mock.Get(documentQuery)
                .Setup(query => query.HasMoreResults)
                .Returns(false);

            docClient = Mock.Of<IDocumentClientEx>();
            Mock.Get(docClient)
                .Setup(ex => ex.CreateDocumentChangeFeedQuery(processorSettings.CollectionSelfLink, It.IsAny<ChangeFeedOptions>()))
                .Returns(documentQuery);

            observer = Mock.Of<IChangeFeedObserver>();
            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>()))
                .Returns(Task.FromResult(false))
                .Callback(cancellationTokenSource.Cancel);

            var checkPointer = new Mock<IPartitionCheckpointer>();
            partitionProcessor = new PartitionProcessor(observer, docClient, processorSettings, checkPointer.Object);
        }

        [Fact]
        public async Task Run_ShouldThrowSplit_IfPartitionGone()
        {
            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Throws(DocumentExceptionHelpers.CreateException("Microsoft.Azure.Documents.GoneException", 1002))
                .ReturnsAsync(feedResponse);

            await Assert.ThrowsAsync<PartitionSplitException>(() => partitionProcessor.RunAsync(cancellationTokenSource.Token));
        }

        [Fact]
        public async Task Run_ShouldThrowSplit_IfPartitionSplitting()
        {
            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Throws(DocumentExceptionHelpers.CreateException("Microsoft.Azure.Documents.GoneException", 1007))
                .ReturnsAsync(feedResponse);

            await Assert.ThrowsAsync<PartitionSplitException>(() => partitionProcessor.RunAsync(cancellationTokenSource.Token));
        }

        [Fact]
        public async Task Run_ShouldReThrow_IfUnknownGoneSubcode()
        {
            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Throws(DocumentExceptionHelpers.CreateException("Microsoft.Azure.Documents.GoneException", 1))
                .ReturnsAsync(feedResponse);

            Exception exception = await Record.ExceptionAsync(() => partitionProcessor.RunAsync(cancellationTokenSource.Token));
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        [Fact]
        public async Task Run_ShouldThrowNotFound_IfPartitionNotFound()
        {
            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Throws(DocumentExceptionHelpers.CreateException("Microsoft.Azure.Documents.NotFoundException", 1))
                .ReturnsAsync(feedResponse);

            Exception exception = await Record.ExceptionAsync(() => partitionProcessor.RunAsync(cancellationTokenSource.Token));
            Assert.IsAssignableFrom<PartitionNotFoundException>(exception);
        }

        [Fact]
        public async Task Run_ShouldReThrow_IfUnknownNotFoundSubcode()
        {
            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Throws(DocumentExceptionHelpers.CreateException("Microsoft.Azure.Documents.NotFoundException", 1002))
                .ReturnsAsync(feedResponse);

            Exception exception = await Record.ExceptionAsync(() => partitionProcessor.RunAsync(cancellationTokenSource.Token));
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        [Fact]
        public async Task Run_ShouldRetry_IfThrottled()
        {
            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Throws(DocumentExceptionHelpers.CreateRequestRateTooLargeException())
                .ReturnsAsync(feedResponse);

            await Assert.ThrowsAsync<TaskCanceledException>(() => partitionProcessor.RunAsync(cancellationTokenSource.Token));

            Mock.Get(documentQuery)
                .Verify(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)), Times.Exactly(2));

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(documents))),
                    Times.Once);
        }
    }
}