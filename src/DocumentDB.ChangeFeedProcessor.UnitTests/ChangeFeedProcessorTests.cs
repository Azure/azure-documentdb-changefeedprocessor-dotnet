//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
    public class ChangeFeedProcessorTests
    {
        private const string collectionLink = "Collection link";
        private const string storeNamePrefix = "Name prefix";
        private static readonly DocumentCollectionInfo collectionInfo = new DocumentCollectionInfo()
        {
            DatabaseName = "DatabaseName",
            CollectionName = "CollectionName",
            Uri = new Uri("https://some.host.com")
        };
        private static readonly Database database = new Database() {
            ResourceId = "someResource"
        };
        private static readonly DocumentCollection collection = new DocumentCollection() {
            ResourceId = "someResource"
        };

        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly IChangeFeedObserver observer;
        private readonly IChangeFeedObserverFactory observerFactory;
        private readonly ChangeFeedProcessorBuilder builder = new ChangeFeedProcessorBuilder();

        public ChangeFeedProcessorTests()
        {
            var leaseQueryMock = new Mock<IDocumentQuery<Document>>();
            var leaseDocumentClient = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(leaseDocumentClient)
                .Setup(c => c.CreateDocumentQuery<Document>(collectionLink,
                    It.Is<SqlQuerySpec>(spec => spec.QueryText == "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)" &&
                                                spec.Parameters.Count == 1 &&
                                                spec.Parameters[0].Name == "@PartitionLeasePrefix" &&
                                                (string)spec.Parameters[0].Value == storeNamePrefix + ".."
                    )))
                .Returns(leaseQueryMock.As<IQueryable<Document>>().Object);
            leaseQueryMock
                .Setup(q => q.HasMoreResults)
                .Returns(false);
            Mock.Get(leaseDocumentClient)
                .Setup(ex => ex.ReadDatabaseAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>()))
                .ReturnsAsync(new ResourceResponse<Database>(database));
            Mock.Get(leaseDocumentClient)
                .Setup(ex => ex.ReadDocumentCollectionAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>()))
                .ReturnsAsync(new ResourceResponse<DocumentCollection>(collection));
            Mock.Get(leaseDocumentClient)
                .Setup(ex => ex.ReadDocumentAsync(It.IsAny<Uri>()))
                .ReturnsAsync(new ResourceResponse<Document>(new Document()));

            var documents = new List<Document> { };
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

            var lease = Mock.Of<ILease>();
            Mock.Get(lease)
                .Setup(l => l.PartitionId)
                .Returns("partitionId");

            var leaseManager = Mock.Of<ILeaseManager>();
            Mock.Get(leaseManager)
                .Setup(manager => manager.AcquireAsync(lease))
                .ReturnsAsync(lease);

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.FromResult(false));

            this.builder
                .WithHostName("someHost")
                .WithFeedDocumentClient(documentClient)
                .WithFeedCollection(collectionInfo)
                .WithChangeFeedHostOptions(new ChangeFeedHostOptions())
                .WithLeaseManager(leaseManager)
                .WithLeaseCollection(collectionInfo)
                .WithLeaseDocumentClient(leaseDocumentClient);

            this.observer = Mock.Of<IChangeFeedObserver>();
            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(false))
                .Callback(cancellationTokenSource.Cancel);
            Mock.Get(observer)
                .Setup(observer => observer.OpenAsync(It.IsAny<ChangeFeedObserverContext>()))
                .Returns(Task.FromResult(false));

            this.observerFactory = Mock.Of<IChangeFeedObserverFactory>();
            Mock.Get(observerFactory)
                .Setup(observer => observer.CreateObserver())
                .Returns(observer);
        }

        [Fact]
        public async Task Start_Then_Estimate()
        {
            this.builder.WithObserverFactory(this.observerFactory);
            var processor = await this.builder.BuildAsync();
            await processor.StartAsync();
            var remainingWorkEstimator = await this.builder.BuildEstimatorAsync();
            await remainingWorkEstimator.GetEstimatedRemainingWork();
        }

        [Fact]
        public async Task Estimate_Then_Start()
        {
            var remainingWorkEstimator = await this.builder.BuildEstimatorAsync();
            await remainingWorkEstimator.GetEstimatedRemainingWork();
            this.builder.WithObserverFactory(this.observerFactory);
            var processor = await this.builder.BuildAsync();
            await processor.StartAsync();
        }
    }
}
