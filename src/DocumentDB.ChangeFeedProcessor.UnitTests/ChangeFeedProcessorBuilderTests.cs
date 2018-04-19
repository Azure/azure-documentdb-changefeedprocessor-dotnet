namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class ChangeFeedProcessorBuilderTests
    {
        private const string collectionLink = "collectionLink";
        private const string storeNamePrefix = "Name prefix";
        private static readonly DocumentCollectionInfo CollectionInfo = new DocumentCollectionInfo
        {
            DatabaseName = "DatabaseName",
            CollectionName = "CollectionName",
            Uri = new Uri("https://some.host.com")
        };
        private static readonly Database database = new Database { ResourceId = "someResource" };
        private static readonly DocumentCollection collection = new DocumentCollection { ResourceId = "someResource" };
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private readonly ChangeFeedProcessorBuilder builder = new ChangeFeedProcessorBuilder();

        public ChangeFeedProcessorBuilderTests()
        {
            this.builder
                .WithHostName("someHost")
                .WithFeedCollection(CollectionInfo)
                .WithLeaseCollection(CollectionInfo);
        }

        [Fact]
        public async Task WithFeedDocumentClient()
        {
            var documentClient = new DocumentClient(new Uri("https://localhost:12345/"), string.Empty);
            var observerFactory = Mock.Of<IChangeFeedObserverFactory>();

            this.builder
                .WithFeedDocumentClient(documentClient)
                .WithLeaseDocumentClient(CreateMockDocumentClient())
                .WithObserverFactory(observerFactory);

            await Assert.ThrowsAsync<HttpRequestException>(async () => await this.builder.BuildAsync());
        }

        [Fact]
        public async Task WithLeaseDocumentClient()
        {
            var documentClient = new DocumentClient(new Uri("https://localhost:12345/"), string.Empty);
            var observerFactory = Mock.Of<IChangeFeedObserverFactory>();

            this.builder
                .WithLeaseDocumentClient(documentClient)
                .WithFeedDocumentClient(CreateMockDocumentClient())
                .WithObserverFactory(observerFactory);

            await Assert.ThrowsAsync<HttpRequestException>(async () => await this.builder.BuildAsync());
        }

        private IChangeFeedDocumentClient CreateMockDocumentClient()
        {
            var documents = new List<Document>();
            var feedResponse = Mock.Of<IFeedResponse<Document>>();
            Mock.Get(feedResponse)
                .Setup(response => response.Count)
                .Returns(documents.Count);
            Mock.Get(feedResponse)
                .Setup(response => response.GetEnumerator())
                .Returns(documents.GetEnumerator());

            var documentQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>();
            Mock.Get(documentQuery)
                .Setup(query => query.HasMoreResults)
                .Returns(false);
            Mock.Get(documentQuery)
                .Setup(query => query.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => feedResponse)
                .Callback(() => cancellationTokenSource.Cancel());

            var leaseQueryMock = new Mock<IDocumentQuery<Document>>();
            leaseQueryMock
                .Setup(q => q.HasMoreResults)
                .Returns(false);

            var documentClient = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(documentClient)
                .Setup(ex => ex.CreateDocumentChangeFeedQuery(collectionLink, It.IsAny<ChangeFeedOptions>()))
                .Returns(documentQuery);
            Mock.Get(documentClient)
                .Setup(ex => ex.ReadDatabaseAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>()))
                .ReturnsAsync(new ResourceResponse<Database>(database));
            Mock.Get(documentClient)
                .Setup(ex => ex.ReadDocumentCollectionAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>()))
                .ReturnsAsync(new ResourceResponse<DocumentCollection>(collection));
            Mock.Get(documentClient)
                .Setup(ex => ex.ReadDocumentAsync(It.IsAny<Uri>()))
                .ReturnsAsync(new ResourceResponse<Document>(new Document()));
            Mock.Get(documentClient)
                .Setup(c => c.CreateDocumentQuery<Document>(collectionLink,
                    It.Is<SqlQuerySpec>(spec => spec.QueryText == "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)" &&
                                                spec.Parameters.Count == 1 &&
                                                spec.Parameters[0].Name == "@PartitionLeasePrefix" &&
                                                (string)spec.Parameters[0].Value == storeNamePrefix + ".."
                    )))
                .Returns(leaseQueryMock.As<IQueryable<Document>>().Object);

            return documentClient;
        }

        [Fact]
        public async Task UseCustomLoadBalancingStrategy()
        {
            var leaseManager = Mock.Of<ILeaseManager>();
            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(It.IsAny<ILease>(), "host"))
                .ReturnsAsync(Mock.Of<ILease>());

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(It.IsAny<ILease>()))
                .Returns(Task.FromResult(false));

            var strategy = Mock.Of<IParitionLoadBalancingStrategy>();

            this.builder
                .WithPartitionLoadBalancingStrategy(strategy)
                .WithFeedDocumentClient(this.CreateMockDocumentClient())
                .WithLeaseDocumentClient(this.CreateMockDocumentClient())
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>())
                .WithLeaseManager(leaseManager);

            var processor = await this.builder.BuildAsync();
            await processor.StartAsync();

            Mock.Get(strategy)
                .Verify(s => s.SelectLeasesToTake(It.IsAny<IEnumerable<ILease>>()), Times.AtLeastOnce);

            await processor.StopAsync();
        }

        [Fact]
        public void UseChangeFeedOptionsWithPartitionKeyRangeId()
        {
            Assert.Throws<ArgumentException>(() =>
                this.builder.WithChangeFeedOptions(new ChangeFeedOptions { PartitionKeyRangeId = "0"}));
        }

        [Fact]
        public void UseChangeFeedOptionsWithPartitionKey()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                var partitionKey = new PartitionKey("id");
                this.builder.WithChangeFeedOptions(new ChangeFeedOptions { PartitionKey = partitionKey });
            });
        }
    }
}
