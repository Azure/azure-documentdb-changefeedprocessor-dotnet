//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.Client;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    [Trait("Category", "Gated")]
    public class RemainingWorkEstimatorTests
    {
        private const string collectionSelfLink = "selfLink";
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ILease lease;
        private readonly IChangeFeedDocumentClient docClient;
        private readonly IChangeFeedDocumentQuery<Document> documentQuery;
        private readonly RemainingWorkEstimator remainingWorkEstimator;
        private readonly ILeaseManager leaseManager;
        private IFeedResponse<Document> feedResponse;
        private List<Document> documents;

        public RemainingWorkEstimatorTests()
        {
            var document = new Document();
            document.SetPropertyValue("_lsn", "10");
            documents = new List<Document> { document };

            feedResponse = Mock.Of<IFeedResponse<Document>>();
            Mock.Get(feedResponse)
                .Setup(response => response.SessionToken)
                .Returns("0:15");

            Mock.Get(feedResponse)
                .Setup(response => response.Count)
                .Returns(documents.Count);

            Mock.Get(feedResponse)
                .Setup(response => response.GetEnumerator())
                .Returns(documents.GetEnumerator());

            lease = Mock.Of<ILease>();
            Mock.Get(lease)
                .Setup(l => l.PartitionId)
                .Returns("partitionId");

            leaseManager = Mock.Of<ILeaseManager>();
            Mock.Get(leaseManager)
                .Setup(manager => manager.ListLeasesAsync())
                .ReturnsAsync(new List<ILease>() { lease });
            
            documentQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>();
            Mock.Get(documentQuery)
                .Setup(query => query.HasMoreResults)
                .Returns(false);

            Mock.Get(documentQuery)
                .Setup(query => query.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => feedResponse)
                .Callback(() => cancellationTokenSource.Cancel());

            docClient = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(docClient)
                .Setup(ex => ex.CreateDocumentChangeFeedQuery(collectionSelfLink, It.IsAny<ChangeFeedOptions>()))
                .Returns(documentQuery);

            remainingWorkEstimator = new RemainingWorkEstimator(leaseManager, docClient, collectionSelfLink);
        }

        [Fact]
        public async Task Estimate_ShouldReturnPendingWork()
        {
            long pendingWork = await remainingWorkEstimator.GetEstimatedRemainingWork();
            Assert.Equal(6, pendingWork);
        }

        [Fact]
        public async Task Estimate_ShouldReturnZero_WhenEmpty()
        {
            var document = new Document();
            documents = new List<Document> { };

            feedResponse = Mock.Of<IFeedResponse<Document>>();
            Mock.Get(feedResponse)
                .Setup(response => response.SessionToken)
                .Returns("0:15");

            Mock.Get(feedResponse)
                .Setup(response => response.Count)
                .Returns(documents.Count);

            Mock.Get(feedResponse)
                .Setup(response => response.GetEnumerator())
                .Returns(documents.GetEnumerator());

            long pendingWork = await remainingWorkEstimator.GetEstimatedRemainingWork();
            Assert.Equal(0, pendingWork);
        }
    }
}
