namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.FeedProcessor
{
    using System;
    using AutoFixture;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class PartitionProcessorFactoryTests
    {
        private readonly IChangeFeedDocumentClient docClient;
        private readonly IChangeFeedObserver observer;

        private readonly Fixture fixture = new Fixture();
        private readonly string collectionSelfLink;

        public PartitionProcessorFactoryTests()
        {
            this.collectionSelfLink = this.fixture.Create<string>();
            this.docClient = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(this.docClient)
                .Setup(ex => ex.CreateDocumentChangeFeedQuery(this.collectionSelfLink, It.IsAny<ChangeFeedOptions>()))
                .Returns(Mock.Of<IChangeFeedDocumentQuery<Document>>());

            this.observer = Mock.Of<IChangeFeedObserver>();
        }

        [Fact]
        public void Create_ShouldPassFeedOptionsToQuery_OnCreation()
        {
            this.fixture.Register(() => DateTime.UtcNow);
            var hostOptions = this.fixture.Create<ChangeFeedProcessorOptions>();
            var partitionKeyId = this.fixture.Create<string>();
            var leaseContinuationToken = this.fixture.Create<string>();

            this.fixture.Register(() => new PartitionKey(this.fixture.Create<string>()));

            var leaseCheckpointer = Mock.Of<ILeaseCheckpointer>();
            var lease = Mock.Of<ILease>();

            Mock.Get(lease)
                .Setup(l => l.PartitionId)
                .Returns(partitionKeyId);
            Mock.Get(lease)
                .Setup(l => l.ContinuationToken)
                .Returns(leaseContinuationToken);

            PartitionProcessorFactory sut = new PartitionProcessorFactory(this.docClient, hostOptions, leaseCheckpointer, this.collectionSelfLink);
            var processor = sut.Create(lease, this.observer);

            Mock.Get(this.docClient)
                .Verify(d => d.CreateDocumentChangeFeedQuery(
                        It.Is<string>(s => s == this.collectionSelfLink),
                        It.Is<ChangeFeedOptions>(options =>
                            options.PartitionKeyRangeId == partitionKeyId &&
                            options.SessionToken == hostOptions.SessionToken &&
                            options.StartFromBeginning == hostOptions.StartFromBeginning &&
                            options.MaxItemCount == hostOptions.MaxItemCount &&
                            options.RequestContinuation == leaseContinuationToken)),
                    Times.Once);
        }
    }
}
