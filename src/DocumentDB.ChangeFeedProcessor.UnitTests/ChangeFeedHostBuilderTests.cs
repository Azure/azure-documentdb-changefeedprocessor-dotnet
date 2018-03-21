using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Processing;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests
{
        using Microsoft.Azure.Documents.Client;
    using Moq;
    using System;
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;

    [Trait("Category", "Gated")]
    public class ChangeFeedHostBuilderTests
    {
        private const string collectionLink = "collectionLink";
        private static readonly DocumentCollectionInfo CollectionInfo = new DocumentCollectionInfo
        {
            DatabaseName = "DatabaseName",
            CollectionName = "CollectionName",
            Uri = new Uri("https://some.host.com")
        };
        private static readonly Database database = new Database { ResourceId = "someResource" };
        private static readonly DocumentCollection collection = new DocumentCollection { ResourceId = "someResource" };
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        private readonly ChangeFeedHostBuilder builder = new ChangeFeedHostBuilder();

        public ChangeFeedHostBuilderTests()
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
            var observerFactory = Mock.Of<Processing.IChangeFeedObserverFactory>();

            this.builder
                .WithFeedDocumentClient(documentClient)
                .WithLeaseDocumentClient(CreateMockDocumentClient())
                .WithObserverFactory(observerFactory);

            await Assert.ThrowsAsync<HttpRequestException>(async () => await this.builder.BuildProcessorAsync());
        }

        [Fact]
        public async Task WithLeaseDocumentClient()
        {
            var documentClient = new DocumentClient(new Uri("https://localhost:12345/"), string.Empty);
            var observerFactory = Mock.Of<Processing.IChangeFeedObserverFactory>();

            this.builder
                .WithLeaseDocumentClient(documentClient)
                .WithFeedDocumentClient(CreateMockDocumentClient())
                .WithObserverFactory(observerFactory);

            await Assert.ThrowsAsync<HttpRequestException>(async () => await this.builder.BuildProcessorAsync());
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

            return documentClient;
        }
    }
}
