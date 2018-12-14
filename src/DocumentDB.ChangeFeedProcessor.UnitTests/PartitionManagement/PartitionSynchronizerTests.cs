//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.Client;
using Moq;
using Newtonsoft.Json;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    [Trait("Category", "Gated")]
    public class PartitionSynchronizerTests
    {
        [Fact]
        public async Task SplitPartitionAsync_ShouldReturnLeasesWithLastKnownContinuation_IfHappyPath()
        {
            const string lastKnowToken = "last know token";

            IEnumerable<PartitionKeyRange> keyRanges = new[]
            {
                new PartitionKeyRange
                {
                    Id = "20", Parents = new Collection<string>(new[] {"10"})
                },
                new PartitionKeyRange
                {
                    Id = "30", Parents = new Collection<string>(new[] {"10"})
                }
            };

            var lease = Mock.Of<ILease>(l => l.PartitionId == "10" && l.ContinuationToken == lastKnowToken);

            var keyRangeResponse = Mock.Of<IFeedResponse<PartitionKeyRange>>(r => r.GetEnumerator() == keyRanges.GetEnumerator());
            IChangeFeedDocumentClient documentClient = Mock.Of<IChangeFeedDocumentClient>(c => c.ReadPartitionKeyRangeFeedAsync(It.IsAny<string>(), It.IsAny<FeedOptions>()) == Task.FromResult(keyRangeResponse));

            var lease20 = Mock.Of<ILease>();
            var lease30 = Mock.Of<ILease>();
            ILeaseManager leaseManager = Mock.Of<ILeaseManager>(m => 
                m.CreateLeaseIfNotExistAsync("20", lastKnowToken) == Task.FromResult(lease20) && 
                m.CreateLeaseIfNotExistAsync("30", lastKnowToken) == Task.FromResult(lease30));
            var leaseContainer = Mock.Of<ILeaseContainer>();

            var sut = new PartitionSynchronizer(documentClient, "collectionlink", leaseContainer, leaseManager, 1, int.MaxValue);
            IEnumerable<ILease> result = await sut.SplitPartitionAsync(lease);
            Assert.NotNull(result);
            Assert.Equal(new [] { lease20, lease30 }, result);
        }

        [Fact]
        public async Task SplitPartitionAsync_ShouldReturnOnlyNewLeases_IfSplitWasAlreadyPerformed()
        {
            const string lastKnowToken = "last know token";

            IEnumerable<PartitionKeyRange> keyRanges = new[]
            {
                new PartitionKeyRange
                {
                    Id = "20", Parents = new Collection<string>(new[] {"10"})
                },
                new PartitionKeyRange
                {
                    Id = "30", Parents = new Collection<string>(new[] {"10"})
                }
            };

            var lease = Mock.Of<ILease>(l => l.PartitionId == "10" && l.ContinuationToken == lastKnowToken);

            var keyRangeResponse = Mock.Of<IFeedResponse<PartitionKeyRange>>(r => r.GetEnumerator() == keyRanges.GetEnumerator());
            IChangeFeedDocumentClient documentClient = Mock.Of<IChangeFeedDocumentClient>(c => c.ReadPartitionKeyRangeFeedAsync(It.IsAny<string>(), It.IsAny<FeedOptions>()) == Task.FromResult(keyRangeResponse));

            var lease20 = Mock.Of<ILease>();
            ILeaseManager leaseManager = Mock.Of<ILeaseManager>(m =>
                m.CreateLeaseIfNotExistAsync("20", lastKnowToken) == Task.FromResult(lease20) &&
                m.CreateLeaseIfNotExistAsync("30", lastKnowToken) == Task.FromResult<ILease>(null));
            var leaseContainer = Mock.Of<ILeaseContainer>();

            var sut = new PartitionSynchronizer(documentClient, "collectionlink", leaseContainer, leaseManager, 1, int.MaxValue);
            IEnumerable<ILease> result = await sut.SplitPartitionAsync(lease);
            Assert.NotNull(result);
            Assert.Equal(new[] { lease20 }, result);
        }

        [Fact]
        public async Task SplitPartitionAsync_ShouldThrow_IfPKRangesReturn0()
        {
            const string lastKnowToken = "last know token";
            const string collectionLink = "collectionLink";

            IEnumerable<PartitionKeyRange> keyRanges = Enumerable.Empty<PartitionKeyRange>();

            var lease = Mock.Of<ILease>(l => l.PartitionId == "10" && l.ContinuationToken == lastKnowToken);

            var keyRangeResponse = Mock.Of<IFeedResponse<PartitionKeyRange>>(r => r.GetEnumerator() == keyRanges.GetEnumerator());
            IChangeFeedDocumentClient documentClient = Mock.Of<IChangeFeedDocumentClient>(c =>
                c.ReadPartitionKeyRangeFeedAsync(It.IsAny<string>(), It.IsAny<FeedOptions>()) == Task.FromResult(keyRangeResponse));

            var lease20 = Mock.Of<ILease>();
            ILeaseManager leaseManager = Mock.Of<ILeaseManager>(m =>
                m.CreateLeaseIfNotExistAsync("20", lastKnowToken) == Task.FromResult(lease20));
            var leaseContainer = Mock.Of<ILeaseContainer>();

            var sut = new PartitionSynchronizer(documentClient, collectionLink, leaseContainer, leaseManager, 1, int.MaxValue);
            Exception exception = await Record.ExceptionAsync(async () => await sut.SplitPartitionAsync(lease));
            Assert.IsAssignableFrom<InvalidOperationException>(exception);
        }
    }
}