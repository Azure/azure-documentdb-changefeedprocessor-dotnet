namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Obsolete
{
    using System;
    using Microsoft.Azure.Documents.Client;
    using Xunit;

    [Trait("Category", "Gated")]
    public class ChangeFeedEventHostTests
    {
#pragma warning disable CS0618 // Type or member is obsolete
        [Fact]
        public void ValidateLegacyOptionsAreUsed()
        {
            DateTime startTime = DateTime.Now;
            var feedOptions = new ChangeFeedOptions
            {
                MaxItemCount = 1,
                StartFromBeginning = true,
                StartTime = startTime,
                RequestContinuation = "RequestContinuation",
                SessionToken = "SessionToken",
            };

            var checkpointFrequency = new CheckpointFrequency { ExplicitCheckpoint = true };
            var hostOptions = new ChangeFeedHostOptions
            {
                LeaseRenewInterval = TimeSpan.FromSeconds(2),
                LeaseAcquireInterval = TimeSpan.FromSeconds(3),
                LeaseExpirationInterval = TimeSpan.FromSeconds(4),
                FeedPollDelay = TimeSpan.FromSeconds(5),
                CheckpointFrequency = checkpointFrequency,
                LeasePrefix = "LeasePrefix",
                MinPartitionCount = 6,
                MaxPartitionCount = 7,
                QueryPartitionsMaxBatchSize = 8,
            };

            var processorOptions = ChangeFeedEventHost.CreateProcessorOptions(feedOptions, hostOptions);

            Assert.Equal(1, processorOptions.MaxItemCount);
            Assert.True(processorOptions.StartFromBeginning);
            Assert.Equal(startTime, processorOptions.StartTime);
            Assert.Equal("RequestContinuation", processorOptions.StartContinuation);
            Assert.Equal("SessionToken", processorOptions.SessionToken);
            Assert.Equal(TimeSpan.FromSeconds(2), processorOptions.LeaseRenewInterval);
            Assert.Equal(TimeSpan.FromSeconds(3), processorOptions.LeaseAcquireInterval);
            Assert.Equal(TimeSpan.FromSeconds(4), processorOptions.LeaseExpirationInterval);
            Assert.Equal(TimeSpan.FromSeconds(5), processorOptions.FeedPollDelay);
            Assert.Equal(checkpointFrequency, processorOptions.CheckpointFrequency);
            Assert.Equal("LeasePrefix", processorOptions.LeasePrefix);
            Assert.Equal(6, processorOptions.MinPartitionCount);
            Assert.Equal(7, processorOptions.MaxPartitionCount);
            Assert.Equal(8, processorOptions.QueryPartitionsMaxBatchSize);
        }

        [Fact]
        public void UseChangeFeedOptionsWithPartitionKeyRangeId()
        {
            Assert.Throws<ArgumentException>(() =>
                new ChangeFeedEventHost(
                    "name",
                    new DocumentCollectionInfo(),
                    new DocumentCollectionInfo(),
                    new ChangeFeedOptions { PartitionKeyRangeId = "0" },
                    new ChangeFeedHostOptions()));
        }

        [Fact]
        public void UseChangeFeedOptionsWithPartitionKey()
        {
            Assert.Throws<ArgumentException>(() =>
                new ChangeFeedEventHost(
                    "name",
                    new DocumentCollectionInfo(),
                    new DocumentCollectionInfo(),
                    new ChangeFeedOptions { PartitionKey = new PartitionKey("id") },
                    new ChangeFeedHostOptions()));
        }

        [Fact]
        public void UseDiscardExistingLeases()
        {
            Assert.Throws<ArgumentException>(() =>
                new ChangeFeedEventHost(
                    "name",
                    new DocumentCollectionInfo(),
                    new DocumentCollectionInfo(),
                    new ChangeFeedHostOptions { DiscardExistingLeases = true }));
        }

        [Fact]
        public void ValidateHostName()
        {
            var hostName = "name";
            var host = new ChangeFeedEventHost(hostName, new DocumentCollectionInfo(), new DocumentCollectionInfo());
            Assert.Equal(hostName, host.HostName);
        }
    }
}
