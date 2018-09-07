//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    [Trait("Category", "Gated")]
    public class RemainingWorkEstimatorTests
    {
        private const string collectionSelfLink = "selfLink";

        [Fact]
        public async Task EstimateTotal_ShouldReturnOne_IfNoLeaseDocumentsCreated()
        {
            IReadOnlyList<ILease> leases = new List<ILease>(0);
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>(),
                collectionSelfLink,
                1);
            long pendingWork = await sut.GetEstimatedRemainingWork();
            Assert.Equal(1, pendingWork);
        }

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
        public async Task EstimateTotal_ShouldReturnZero_WhenEmptyFeedResponse()
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
        public async Task EstimateTotal_ShouldReturnOne_WhenNothingSucceeds()
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
            Assert.Equal(1, pendingWork);
        }

        [Fact]
        public async Task EstimateTotal_ShouldPropagateExecption_IfUnknownExceptionHappened()
        {
            IReadOnlyList<ILease> leases = new List<ILease>(0);
            var leaseManager = new Mock<ILeaseManager>();
            leaseManager.Setup(m => m.ListAllLeasesAsync()).ThrowsAsync(new InvalidOperationException());
            var sut = new RemainingWorkEstimator(
                leaseManager.Object,
                Mock.Of<IChangeFeedDocumentClient>(),
                collectionSelfLink,
                1);
            await Assert.ThrowsAsync<InvalidOperationException>(sut.GetEstimatedRemainingWork);
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

            var pendingWork = await sut.GetEstimatedRemainingWorkPerPartition();
            Assert.Contains(pendingWork, work => work.PartitionKeyRangeId == "1" && work.RemainingWork == 6);
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

            var pendingWork = await sut.GetEstimatedRemainingWorkPerPartition();
            Assert.Contains(pendingWork, work => work.PartitionKeyRangeId == "1" && work.RemainingWork == 3);
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
            var pendingWork = await sut.GetEstimatedRemainingWorkPerPartition();
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

            var pendingWork = await sut.GetEstimatedRemainingWorkPerPartition();
            Assert.Contains(pendingWork, work => work.PartitionKeyRangeId == "1" && work.RemainingWork == 0);
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

            var pendingWork = await sut.GetEstimatedRemainingWorkPerPartition();
            Assert.Contains(pendingWork, work => work.PartitionKeyRangeId == "1" && work.RemainingWork == 6);
            Assert.Contains(pendingWork, work => work.PartitionKeyRangeId == "2" && work.RemainingWork == 1);
            Assert.Equal(2, pendingWork.Count);
        }

        [Fact]
        public async Task EstimatePerPartition_ShouldRunInParallel_IfDegreeOfParallelismIsTwo()
        {
            IReadOnlyList<ILease> leases = new List<ILease>
            {
                Mock.Of<ILease>(l => l.PartitionId == "1" && l.ContinuationToken == "100"),
                Mock.Of<ILease>(l => l.PartitionId == "2" && l.ContinuationToken == "200")
            };

            TaskCompletionSource<bool> cts1 = new TaskCompletionSource<bool>();
            TaskCompletionSource<bool> cts2 = new TaskCompletionSource<bool>();
            TaskCompletionSource<bool> ctsAll = new TaskCompletionSource<bool>();
            var sut = new RemainingWorkEstimator(
                Mock.Of<ILeaseManager>(m => m.ListAllLeasesAsync() == Task.FromResult(leases)),
                Mock.Of<IChangeFeedDocumentClient>()
                    .SetupQueryResponse("1", "100", "101", "1:106", async r =>
                    {
                        cts1.SetResult(true);
                        await ctsAll.Task;
                        return r;
                    })
                    .SetupQueryResponse("2", "200", "201", "2:201", async r =>
                    {
                        cts2.SetResult(true);
                        await ctsAll.Task;
                        return r;
                    }),
                collectionSelfLink,
                2);

            var workPerPartitionTask = sut.GetEstimatedRemainingWorkPerPartition();

            await Task.WhenAll(cts1.Task, cts2.Task);
            ctsAll.SetResult(true);

            var pendingWork = await workPerPartitionTask;
            Assert.Contains(pendingWork, work => work.PartitionKeyRangeId == "1" && work.RemainingWork == 6);
            Assert.Contains(pendingWork, work => work.PartitionKeyRangeId == "2" && work.RemainingWork == 1);
            Assert.Equal(2, pendingWork.Count);
        }

    }
}