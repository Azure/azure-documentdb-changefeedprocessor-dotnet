namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Moq;
    using Xunit;

    [Trait("Category", "Gated")]
    public class ChangeFeedProcessorBuilderTests
    {
        private const string collectionLink = "collectionLink";
        private static readonly DocumentCollectionInfo CollectionInfo = new DocumentCollectionInfo
        {
            DatabaseName = "DatabaseName",
            CollectionName = "CollectionName",
            Uri = new Uri("https://some.host.com")
        };
        private static readonly Database database = new Database { ResourceId = "databaseRid" };
        private static readonly DocumentCollection collection = MockHelpers.CreateCollection("colectionId", "collectionRid");

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
            return CreateMockDocumentClient(collection);
        }

        private IChangeFeedDocumentClient CreateMockDocumentClient(DocumentCollection collection)
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
                .Setup(c => c.CreateDocumentQuery<Document>(collectionLink,
                    It.Is<SqlQuerySpec>(spec => spec.QueryText == "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)" &&
                                                spec.Parameters.Count == 1 &&
                                                spec.Parameters[0].Name == "@PartitionLeasePrefix" &&
                                                (string)spec.Parameters[0].Value == $"{CollectionInfo.Uri.Host}_{database.ResourceId}_{collection.ResourceId}.."
                    ), It.IsAny<FeedOptions>()))
                .Returns(leaseQueryMock.As<IQueryable<Document>>().Object);

            return documentClient;
        }

        [Fact]
        public async Task UseCustomLoadBalancingStrategy()
        {
            var leaseStoreManager = Mock.Of<ILeaseStoreManager>();
            Mock.Get(leaseStoreManager)
                .Setup(store => store.IsInitializedAsync())
                .ReturnsAsync(true);
            Mock.Get(leaseStoreManager)
                .Setup(manager => manager.AcquireAsync(It.IsAny<ILease>()))
                .ReturnsAsync(Mock.Of<ILease>());
            Mock.Get(leaseStoreManager)
                .Setup(manager => manager.ReleaseAsync(It.IsAny<ILease>()))
                .Returns(Task.CompletedTask);

            var strategy = Mock.Of<IParitionLoadBalancingStrategy>();

            this.builder
                .WithPartitionLoadBalancingStrategy(strategy)
                .WithFeedDocumentClient(this.CreateMockDocumentClient())
                .WithLeaseDocumentClient(this.CreateMockDocumentClient())
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>())
                .WithLeaseStoreManager(leaseStoreManager);

            var processor = await this.builder.BuildAsync();
            await processor.StartAsync();

            Mock.Get(strategy)
                .Verify(s => s.SelectLeasesToTake(It.IsAny<IEnumerable<ILease>>()), Times.AtLeastOnce);

            await processor.StopAsync();
        }

        [Fact]
        public async Task BuildThrowsWhenLeaseCollectionPartitionedNotById()
        {
            SetupBuilderForPartitionedLeaseCollection("/not_id");
            await Assert.ThrowsAsync<ArgumentException>(async () => await builder.BuildAsync() );
        }

        [Fact]
        public async Task BuildWhenLeaseCollectionPartitionedById()
        {
            SetupBuilderForPartitionedLeaseCollection("/id");
            await this.builder.BuildAsync();
        }

        [Fact]
        public async Task BuildThrowsWhenNoneOfLeaseCollectionInfoOrLeaseStoreManagerSpecified()
        {
            var builder = new ChangeFeedProcessorBuilder()
                .WithHostName("host")
                .WithFeedCollection(CollectionInfo)
                .WithFeedDocumentClient(CreateMockDocumentClient())
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>());

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await builder.BuildAsync());
        }

        [Fact]
        public async Task BuildWhenOnlyLeaseStoreManagerSpecified()
        {
            var builder = new ChangeFeedProcessorBuilder()
                .WithHostName("host")
                .WithFeedCollection(CollectionInfo)
                .WithFeedDocumentClient(CreateMockDocumentClient())
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>())
                .WithLeaseStoreManager(Mock.Of<ILeaseStoreManager>());

            await builder.BuildAsync();
        }

        [Fact]
        public async Task BuildEstimatorThrowsWhenLeaseCollectionPartitionedNotById()
        {
            SetupBuilderForPartitionedLeaseCollection("/not_id");
            await Assert.ThrowsAsync<ArgumentException>(async () => await builder.BuildEstimatorAsync());
        }

        [Fact]
        public async Task BuildEstimatorWhenLeaseCollectionPartitionedById()
        {
            SetupBuilderForPartitionedLeaseCollection("/id");
            await this.builder.BuildEstimatorAsync();
        }

        [Fact]
        public async Task BuildEstimatorThrowsWhenNoneOfLeaseCollectionInfoOrLeaseStoreManagerSpecified()
        {
            var builder = new ChangeFeedProcessorBuilder()
                .WithHostName("host")
                .WithFeedCollection(CollectionInfo)
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>());

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await builder.BuildEstimatorAsync());
        }

        [Fact]
        public async Task BuildEstimatorWhenOnlyLeaseStoreManagerSpecified()
        {
            var builder = new ChangeFeedProcessorBuilder()
                .WithHostName("host")
                .WithFeedCollection(CollectionInfo)
                .WithFeedDocumentClient(CreateMockDocumentClient())
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>())
                .WithLeaseStoreManager(Mock.Of<ILeaseStoreManager>());

            await builder.BuildEstimatorAsync();
        }

        [Fact]
        public async Task BuildPassesEnableCrossPartitionQuery_WhenLeaseCollectionIsPartitionedById()
        {
            var leaseCollection = MockHelpers.CreateCollection(
                "collectionId",
                "collectionRid",
                new PartitionKeyDefinition { Paths = { "/id" } },
                collectionLink);

            var leaseClient = this.CreateMockDocumentClient(collection);
            Mock.Get(leaseClient)
                .Setup(c => c.ReadDocumentCollectionAsync(
                    It.IsAny<Uri>(),
                    It.IsAny<RequestOptions>()))
                .ReturnsAsync(new ResourceResponse<DocumentCollection>(leaseCollection));

            this.builder
                .WithFeedDocumentClient(this.CreateMockDocumentClient())
                .WithLeaseDocumentClient(leaseClient)
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>());
            await this.builder.BuildAsync();

            await this.builder.LeaseStoreManager.GetAllLeasesAsync();

            Mock.Get(leaseClient)
                .Verify(c => c.CreateDocumentQuery<Document>(
                    It.IsAny<string>(),
                    It.IsAny<SqlQuerySpec>(),
                    It.Is<FeedOptions>(opt => opt.EnableCrossPartitionQuery == true)));
        }

        [Fact]
        public async Task BuildPassesPartitionKey_WhenLeaseCollectionIsPartitionedById()
        {
            var leaseCollection = MockHelpers.CreateCollection(
                "collectionId",
                "collectionRid",
                new PartitionKeyDefinition { Paths = { "/id" } },
                collectionLink);

            var lease = Mock.Of<ILease>();
            Mock.Get(lease)
                .SetupGet(l => l.Id)
                .Returns("leaseId");

            var leaseClient = this.CreateMockDocumentClient(collection);
            Mock.Get(leaseClient)
                .Setup(c => c.ReadDocumentCollectionAsync(
                    It.IsAny<Uri>(),
                    It.IsAny<RequestOptions>()))
                .ReturnsAsync(new ResourceResponse<DocumentCollection>(leaseCollection));
            Mock.Get(leaseClient)
                .Setup(c => c.ReadDocumentAsync(
                    It.IsAny<Uri>(),
                    It.IsAny<RequestOptions>(),
                    It.IsAny<CancellationToken>()))
                .Callback((Uri uri, RequestOptions options, CancellationToken token) =>
                {
                    if (new PartitionKey(lease.Id).Equals(options.PartitionKey))
                        throw DocumentExceptionHelpers.CreateNotFoundException();   // Success code path: cause lease lost.
                    throw new Exception("Failure");
                });
                
            this.builder
                .WithFeedDocumentClient(this.CreateMockDocumentClient())
                .WithLeaseDocumentClient(leaseClient)
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>());
            await this.builder.BuildAsync();

            Exception exception = await Record.ExceptionAsync(() => this.builder.LeaseStoreManager.ReleaseAsync(lease));
            Assert.Equal(typeof(LeaseLostException), exception.GetType());
        }

        private void SetupBuilderForPartitionedLeaseCollection(string partitionKey)
        {
            var partitionedCollection = MockHelpers.CreateCollection(
                collection.Id,
                collection.ResourceId,
                new PartitionKeyDefinition { Paths = { partitionKey } });
            this.builder
                .WithFeedDocumentClient(this.CreateMockDocumentClient())
                .WithLeaseDocumentClient(this.CreateMockDocumentClient(partitionedCollection))
                .WithObserverFactory(Mock.Of<IChangeFeedObserverFactory>());
        }
    }
}
