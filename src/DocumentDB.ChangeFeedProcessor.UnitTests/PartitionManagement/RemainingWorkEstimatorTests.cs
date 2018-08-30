//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Estimator;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
using Microsoft.Azure.Documents.Client;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    [Trait("Category", "Gated")]
    public class RemainingWorkEstimatorTests
    {
        private const string collectionSelfLink = "selfLink";

        [Fact]
        public async Task EstimateTotal_ShouldReturnPendingWork_IfOnePartition()
        {
            IReadOnlyList<ILease> leases = new List<ILease> { Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100") };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", "101", "1:106"),
                collectionSelfLink,
                1);
            long pendingWork = await sut.GetEstimatedRemainingWork();
            Assert.Equal(6, pendingWork);
        }

        [Fact]
        public async Task EstimateTotal_ShouldReturnZero_WhenEmptyResponse()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100")
            };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", null, "1:100"),
                collectionSelfLink,
                1);
            long pendingWork = await sut.GetEstimatedRemainingWork();
            Assert.Equal(0, pendingWork);
        }

        [Fact]
        public async Task EstimateTotal_ShouldReturnPendingWork_IfMultiplePartitions()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100"),
                Mock.Of<ILease>(l => l.PartitionId == "2" && l.ContinuationToken == "200")
            };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", "101", "1:106")
                    .SetupQueryResponse("2", "200", "201", "2:201"),
                collectionSelfLink,
                1);
            long pendingWork = await sut.GetEstimatedRemainingWork();
            Assert.Equal(7, pendingWork);
        }

        [Fact]
        public async Task EstimateTotal_ShouldReturnPendingWork_IfMultiplePartitionsButOneFails()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100"),
                Mock.Of<ILease>(l => l.PartitionId == "2" && l.ContinuationToken == "200")
            };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", "101", "1:103")
                    .SetupQueryResponseFailure("2", "200"),
                collectionSelfLink,
                1);
            long pendingWork = await sut.GetEstimatedRemainingWork();
            Assert.Equal(3, pendingWork);
        }

        [Fact]
        public async Task EstimateTotal_ShouldReturnZero_WhenNothingSucceeds()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100")
            };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponseFailure("1", "100"),
                collectionSelfLink,
                1);
            long pendingWork = await sut.GetEstimatedRemainingWork();
            Assert.Equal(0, pendingWork);
        }

        [Fact]
        public async Task EstimatePerPartition_ShouldReturnPendingWork_IfOnePartition()
        {
            IReadOnlyList<ILease> leases = new List<ILease> { Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100") };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", "101", "1:106"),
                collectionSelfLink,
                1);

            RemainingPartitionWork[] pendingWork = await sut.GetEstimatedPartitionsRemainingWork();
            Assert.Contains(pendingWork, work => work.PartitionRangeId == "1" && work.RemainingWork == 6);
            Assert.Single(pendingWork);
        }

        [Fact]
        public async Task EstimatePerPartition_ShouldReturnPendingWork_IfMultiplePartitionsButOneFails()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100"),
                Mock.Of<ILease>(l => l.PartitionId == "2" && l.ContinuationToken == "200")
            };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", "101", "1:103")
                    .SetupQueryResponseFailure("2", "200"),
                collectionSelfLink,
                1);

            RemainingPartitionWork[] pendingWork = await sut.GetEstimatedPartitionsRemainingWork();
            Assert.Contains(pendingWork, work => work.PartitionRangeId == "1" && work.RemainingWork == 3);
            Assert.Single(pendingWork);
        }

        [Fact]
        public async Task EstimatePerPartition_ShouldReturnEmpty_WhenNothingSucceeds()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100")
            };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponseFailure("1", "100"),
                collectionSelfLink,
                1);
            RemainingPartitionWork[] pendingWork = await sut.GetEstimatedPartitionsRemainingWork();
            Assert.Empty(pendingWork);
        }

        [Fact]
        public async Task EstimatePerPartition_ShouldReturnZero_WhenEmptyResponse()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100")
            };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", null, "1:100"),
                collectionSelfLink,
                1);
            RemainingPartitionWork[] pendingWork = await sut.GetEstimatedPartitionsRemainingWork();
            Assert.Contains(pendingWork, work => work.PartitionRangeId == "1" && work.RemainingWork == 0);
            Assert.Single(pendingWork);
        }

        [Fact]
        public async Task EstimatePerPartition_ShouldReturnPendingWork_IfMultiplePartitions()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100"),
                Mock.Of<ILease>(l => l.PartitionId == "2" && l.ContinuationToken == "200")
            };
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", "101", "1:106")
                    .SetupQueryResponse("2", "200", "201", "2:201"),
                collectionSelfLink,
                1);
            RemainingPartitionWork[] pendingWork = await sut.GetEstimatedPartitionsRemainingWork();
            Assert.Contains(pendingWork, work => work.PartitionRangeId == "1" && work.RemainingWork == 6);
            Assert.Contains(pendingWork, work => work.PartitionRangeId == "2" && work.RemainingWork == 1);
            Assert.Equal(2, pendingWork.Length);
        }

    }

    internal static class ChangeFeedDocumentClientExtensions
    {
        public static IChangeFeedDocumentClient SetupQueryResponse(this IChangeFeedDocumentClient client, string pid, string token, string documentLsn, string targetSession)
        {
            IList<Document> docs;
            if (documentLsn != null)
            {
                Document document = new Document();
                document.SetPropertyValue("_lsn", documentLsn);
                docs = new List<Document> { document };
            }
            else
            {
                docs = new List<Document>();
            }

            var feedResponse = Mock.Of<IFeedResponse<Document>>(f => 
                f.SessionToken == targetSession &&
                f.Count == docs.Count && 
                f.GetEnumerator() == docs.GetEnumerator());
            var documentQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>(q => q.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()) == Task.FromResult(feedResponse));

            Mock.Get(client)
                .Setup(c => c.CreateDocumentChangeFeedQuery(
                                It.IsAny<string>(),
                                It.Is<ChangeFeedOptions>(o => o.PartitionKeyRangeId == pid && o.RequestContinuation == token)))
                .Returns(documentQuery);

            return client;
        }
        public static IChangeFeedDocumentClient SetupQueryResponseFailure(this IChangeFeedDocumentClient client, string pid, string token)
        {
            var documentQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>();
            Mock.Get(documentQuery)
                .Setup(q => q.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()))
                .ThrowsAsync(DocumentExceptionHelpers.CreateNotFoundException());

            Mock.Get(client)
                .Setup(c => c.CreateDocumentChangeFeedQuery(
                    It.IsAny<string>(),
                    It.Is<ChangeFeedOptions>(o => o.PartitionKeyRangeId == pid && o.RequestContinuation == token)))
                .Returns(documentQuery);

            return client;
        }
    }
}
