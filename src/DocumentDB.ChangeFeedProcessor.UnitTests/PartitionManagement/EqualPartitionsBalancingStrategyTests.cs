//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    [Trait("Category", "Gated")]
    public class EqualPartitionsBalancingStrategyTests
    {
        private const string ownerSelf = "self";
        private const string owner1 = "owner 1";
        private const string owner2 = "owner 2";
        private const string ownerNone = null;

        [Fact]
        public void CalculateLeasesToTake_NoLeases_ReturnsEmpty()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var leasesToTake = strategy.SelectLeasesToTake(Enumerable.Empty<ILease>());
            Assert.Empty(leasesToTake);
        }

        [Fact]
        public void CalculateLeasesToTake_OwnLeasesOnly_ReturnsEmpty()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var leasesToTake = strategy.SelectLeasesToTake(new []{ CreateLease(ownerSelf, "1"), CreateLease(ownerSelf, "2") });
            Assert.Empty(leasesToTake);
        }

        [Fact]
        public void CalculateLeasesToTake_NotOwnedLeasesOnly_ReturnsAll()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new HashSet<ILease> { CreateLease(ownerNone, "1"), CreateLease(ownerNone, "2") };
            var leasesToTake = strategy.SelectLeasesToTake(allLeases);
            Assert.Equal(allLeases, new HashSet<ILease>(leasesToTake));
        }

        [Fact]
        public void CalculateLeasesToTake_ExpiredLeasesOnly_ReturnsAll()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new HashSet<ILease> { CreateExpiredLease(ownerSelf, "1"), CreateExpiredLease(owner1, "2") };
            var leasesToTake = strategy.SelectLeasesToTake(allLeases);
            Assert.Equal(allLeases, new HashSet<ILease>(leasesToTake));
        }

        [Fact]
        public void CalculateLeasesToTake_OtherSingleOwnerTwoLeasesOnly_ReturnsOne()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new HashSet<ILease> { CreateLease(owner1, "1"), CreateLease(owner1, "2") };
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Single(leasesToTake);
            Assert.Subset(allLeases, new HashSet<ILease>(leasesToTake));
        }

        [Fact]
        public void CalculateLeasesToTake_ExpiredAndOtherOwner_ReturnsExpiredOnly()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            ILease expiredLease = CreateExpiredLease(owner1, "4");
            var allLeases = new HashSet<ILease>
            {
                CreateLease(owner1, "1"),
                CreateLease(owner1, "2"),
                CreateLease(owner1, "3"),
                expiredLease
            };
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Single(leasesToTake);
            Assert.Contains(expiredLease, leasesToTake);
        }

        [Fact]
        public void CalculateLeasesToTake_ExpiredAndOtherSingleOwner_ReturnsHalfOfExpiredRoundedUp()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases1 = new List<ILease>();
            allLeases1.Add(CreateLease(owner1, "0"));
            allLeases1.AddRange(Enumerable.Range(1, 10).Select(index => CreateExpiredLease(owner1, index.ToString())));
            var allLeases = allLeases1;
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Equal(6, leasesToTake.Count);
        }

        [Fact]
        public void CalculateLeasesToTake_MinPartitionsSet_ReturnsMinCountOfPartitions()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy(minPartitionCount: 7);
            var allLeases1 = new List<ILease>();
            allLeases1.Add(CreateLease(owner1, "0"));
            allLeases1.AddRange(Enumerable.Range(1, 10).Select(index => CreateExpiredLease(owner1, index.ToString())));
            var allLeases = allLeases1;
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Equal(7, leasesToTake.Count);
        }

        [Fact]
        public void CalculateLeasesToTake_MaxPartitionsSet_ReturnsMaxCountOfPartitions()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy(maxPartitionCount: 3);
            var allLeases1 = new List<ILease>();
            allLeases1.Add(CreateLease(owner1, "0"));
            allLeases1.AddRange(Enumerable.Range(1, 10).Select(index => CreateExpiredLease(owner1, index.ToString())));
            var allLeases = allLeases1;
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Equal(3, leasesToTake.Count);
        }

        [Fact]
        public void CalculateLeasesToTake_TwoOwners_ReturnsStolenFromLargerOwner()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new List<ILease>();
            allLeases.AddRange(Enumerable.Range(1, 5).Select(index => CreateLease(owner1, "A" + index.ToString())));
            allLeases.AddRange(Enumerable.Range(1, 10).Select(index => CreateLease(owner2, "B" + index.ToString())));
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            ILease stolenLease = Assert.Single(leasesToTake);
            Assert.StartsWith("B", stolenLease.PartitionId);
        }

        [Fact]
        public void CalculateLeasesToTake_HavingMoreThanOtherOwner_ReturnsEmpty()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new List<ILease>();
            allLeases.AddRange(Enumerable.Range(1, 5).Select(index => CreateLease(owner1, "A" + index.ToString())));
            allLeases.AddRange(Enumerable.Range(1, 6).Select(index => CreateLease(ownerSelf, "B" + index.ToString())));
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Empty(leasesToTake);
        }

        [Fact]
        public void CalculateLeasesToTake_HavingEqualThanOtherOwner_ReturnsEmpty()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new List<ILease>();
            allLeases.AddRange(Enumerable.Range(1, 5).Select(index => CreateLease(owner1, "A" + index.ToString())));
            allLeases.AddRange(Enumerable.Range(1, 5).Select(index => CreateLease(ownerSelf, "B" + index.ToString())));
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Empty(leasesToTake);
        }

        [Fact]
        public void CalculateLeasesToTake_AllOtherOwnersEqualTargetCount_ReturnsEmpty()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new List<ILease>();
            allLeases.AddRange(Enumerable.Range(1, 4).Select(index => CreateLease(owner1, "A" + index.ToString())));
            allLeases.AddRange(Enumerable.Range(1, 3).Select(index => CreateLease(ownerSelf, "B" + index.ToString())));
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Empty(leasesToTake);
        }

        [Fact]
        public void CalculateLeasesToTake_OtherOwnerGreaterThanTargetCount_ReturnsLease()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new List<ILease>();
            allLeases.AddRange(Enumerable.Range(1, 4).Select(index => CreateLease(owner1, "A" + index.ToString())));
            allLeases.AddRange(Enumerable.Range(1, 2).Select(index => CreateLease(ownerSelf, "B" + index.ToString())));
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            ILease stolenLease = Assert.Single(leasesToTake);
            Assert.StartsWith("A", stolenLease.PartitionId);
        }

        [Fact]
        public void CalculateLeasesToTake_NeedTwoAndOtherOwnersEqualThanTargetCount_ReturnsLease()
        {
            EqualPartitionsBalancingStrategy strategy = CreateStrategy();
            var allLeases = new List<ILease>();
            allLeases.AddRange(Enumerable.Range(1, 10).Select(index => CreateLease(owner1, "A" + index.ToString())));
            allLeases.AddRange(Enumerable.Range(1, 10).Select(index => CreateLease(owner2, "B" + index.ToString())));
            allLeases.AddRange(Enumerable.Range(1, 8).Select(index => CreateLease(ownerSelf, "C" + index.ToString())));
            var leasesToTake = strategy.SelectLeasesToTake(allLeases).ToList();
            Assert.Single(leasesToTake);
        }

        private EqualPartitionsBalancingStrategy CreateStrategy(int minPartitionCount = 0, int maxPartitionCount = 0)
        {
            TimeSpan leaseExpirationInterval = TimeSpan.FromMinutes(10);
            return new EqualPartitionsBalancingStrategy(ownerSelf, minPartitionCount, maxPartitionCount, leaseExpirationInterval);
        }

        private ILease CreateLease(string owner, string partitionId)
        {
            return CreateLease(owner, partitionId, DateTime.UtcNow);
        }

        private ILease CreateExpiredLease(string owner, string partitionId)
        {
            return CreateLease(owner, partitionId, DateTime.UtcNow.AddYears(-1));
        }

        private static ILease CreateLease(string owner, string partitionId, DateTime timestamp)
        {
            var lease = Mock.Of<ILease>();
            Mock.Get(lease).Setup(l => l.Owner).Returns(owner);
            Mock.Get(lease).Setup(l => l.PartitionId).Returns(partitionId);
            Mock.Get(lease).Setup(l => l.Timestamp).Returns(timestamp);
            return lease;
        }
    }
}
