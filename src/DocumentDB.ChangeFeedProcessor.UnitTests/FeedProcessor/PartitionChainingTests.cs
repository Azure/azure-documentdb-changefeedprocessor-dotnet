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
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.Client;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class PartitionChainingTests
    {
        private readonly ProcessorSettings processorSettings;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly PartitionProcessor partitionProcessor;
        private readonly IChangeFeedDocumentClient docClient;
        private readonly IChangeFeedDocumentQuery<Document> documentQuery;
        private readonly IFeedResponse<Document> feedResponse1, feedResponse2;
        private readonly IChangeFeedObserver observer;
        private readonly List<Document> batch1, batch2;

        public PartitionChainingTests()
        {
            processorSettings = new ProcessorSettings
            {
                CollectionSelfLink = "selfLink",
                MaxItemCount = 5,
                FeedPollDelay = TimeSpan.FromMilliseconds(16),
                PartitionKeyRangeId = "keyRangeId",
                StartContinuation = "initialToken"
            };

            var document = new Document();
            batch1 = new List<Document> { document };

            document = new Document();
            batch2 = new List<Document> { document };

            feedResponse1 = Mock.Of<IFeedResponse<Document>>();
            Mock.Get(feedResponse1)
                .Setup(response => response.Count)
                .Returns(batch1.Count);
            Mock.Get(feedResponse1)
                .SetupSequence(response => response.ResponseContinuation)
                .Returns("token1");
            Mock.Get(feedResponse1)
                .SetupSequence(response => response.GetEnumerator())
                .Returns(batch1.GetEnumerator());

            feedResponse2 = Mock.Of<IFeedResponse<Document>>();
            Mock.Get(feedResponse2)
                .Setup(response => response.Count)
                .Returns(batch2.Count);
            Mock.Get(feedResponse2)
                .SetupSequence(response => response.ResponseContinuation)
                .Returns("token2");
            Mock.Get(feedResponse2)
                .SetupSequence(response => response.GetEnumerator())
                .Returns(batch2.GetEnumerator());

            documentQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>();
            Mock.Get(documentQuery)
                .SetupSequence(query => query.HasMoreResults)
                .Returns(true)
                .Returns(false);

            Mock.Get(documentQuery)
                .SetupSequence(query => query.ExecuteNextAsync<Document>(It.Is<CancellationToken>(token => token == cancellationTokenSource.Token)))
                .Returns(Task.FromResult(feedResponse1))
                .Returns(Task.FromResult(feedResponse2));

            docClient = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(docClient)
                .Setup(ex => ex.CreateDocumentChangeFeedQuery(processorSettings.CollectionSelfLink, It.IsAny<ChangeFeedOptions>()))
                .Returns(documentQuery);

            observer = Mock.Of<IChangeFeedObserver>();
            var checkPointer = new Mock<IPartitionCheckpointer>();
            partitionProcessor = new PartitionProcessor(observer, docClient, processorSettings, checkPointer.Object);

            var i = 0;
            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask)
                .Callback(() =>
                {
                    if (++i == 2)
                        cancellationTokenSource.Cancel();
                });
        }

        [Fact]
        public async Task Run_ShouldReturnAllBatches_IfHasMoreResults()
        {
            // intentionally break the processor delay
            processorSettings.FeedPollDelay = TimeSpan.FromMilliseconds(-1);

            await Assert.ThrowsAsync<TaskCanceledException>(() => partitionProcessor.RunAsync(cancellationTokenSource.Token));

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(batch1)),
                            It.IsAny<CancellationToken>()),
                    Times.Once);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(
                            It.Is<ChangeFeedObserverContext>(context => context.PartitionKeyRangeId == processorSettings.PartitionKeyRangeId),
                            It.Is<IReadOnlyList<Document>>(list => list.SequenceEqual(batch2)),
                            It.IsAny<CancellationToken>()),
                    Times.Once);

            Mock.Get(observer)
                .Verify(feedObserver => feedObserver
                        .ProcessChangesAsync(It.IsAny<ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()),
                    Times.Exactly(2));
        }
    }
}