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
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
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
        private static readonly DocumentCollection collection = MockHelpers.CreateCollection("someResource", "someResourceRid");

        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly IChangeFeedObserver observer;
        private readonly IChangeFeedObserverFactory observerFactory;
        private readonly ILease lease;
        private readonly ILeaseStoreManager leaseStoreManager;
        private readonly ChangeFeedProcessorBuilder builder = new ChangeFeedProcessorBuilder();

        private Action<string, ChangeFeedOptions> createDocumentChangeFeedQueryCallback = (string s, ChangeFeedOptions o) => { };

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
                    ), null))
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
                .Callback(cancellationTokenSource.Cancel);

            var documentClient = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(documentClient)
                .Setup(ex => ex.CreateDocumentChangeFeedQuery(It.IsAny<string>(), It.IsAny<ChangeFeedOptions>()))
                .Callback((string s, ChangeFeedOptions o) => this.createDocumentChangeFeedQueryCallback(s, o))
                .Returns(documentQuery);
            Mock.Get(documentClient)
                .Setup(ex => ex.ReadDatabaseAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>()))
                .ReturnsAsync(new ResourceResponse<Database>(database));
            Mock.Get(documentClient)
                .Setup(ex => ex.ReadDocumentCollectionAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>()))
                .ReturnsAsync(new ResourceResponse<DocumentCollection>(collection));

            this.lease = Mock.Of<ILease>();
            Mock.Get(this.lease)
                .Setup(l => l.PartitionId)
                .Returns("partitionId");

            var leaseStore = Mock.Of<ILeaseStore>();

            this.leaseStoreManager = Mock.Of<ILeaseStoreManager>();
            Mock.Get(this.leaseStoreManager)
                .Setup(store => store.IsInitializedAsync())
                .ReturnsAsync(true);
            Mock.Get(this.leaseStoreManager)
                .Setup(manager => manager.AcquireAsync(lease))
                .ReturnsAsync(lease);
            Mock.Get(this.leaseStoreManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.CompletedTask);

            this.builder
                .WithHostName("someHost")
                .WithFeedDocumentClient(documentClient)
                .WithFeedCollection(collectionInfo)
                .WithProcessorOptions(new ChangeFeedProcessorOptions())
                .WithLeaseStoreManager(leaseStoreManager)
                .WithLeaseCollection(collectionInfo)
                .WithLeaseDocumentClient(leaseDocumentClient);

            this.observer = Mock.Of<IChangeFeedObserver>();
            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask)
                .Callback(cancellationTokenSource.Cancel);
            Mock.Get(observer)
                .Setup(observer => observer.OpenAsync(It.IsAny<ChangeFeedObserverContext>()))
                .Returns(Task.CompletedTask);

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

        [Fact]
        public async Task PassesStartContinuation_WhenLeaseDoesNotHaveContinuation()
        {
            Mock.Get(this.leaseStoreManager)
                .Setup(manager => manager.GetOwnedLeasesAsync())
                .ReturnsAsync(new List<ILease> { this.lease });

            var startContinuation = "start";
            this.createDocumentChangeFeedQueryCallback = (string collectionLink, ChangeFeedOptions options) => 
            {
                Assert.Equal(options.RequestContinuation, startContinuation);
                throw new InjectedException();
            };

            this.builder
                .WithObserverFactory(this.observerFactory)
                .WithProcessorOptions(new ChangeFeedProcessorOptions { StartContinuation = startContinuation });
            var processor = await this.builder.BuildAsync();

            // Since CreateDocumentChangeFeedQueryCallback always throws, 
            // processor.StartAsync never goes to async mode, everything is within sync "StartAsync/RunAsync" code path.
            var exception = await Record.ExceptionAsync(async () => await processor.StartAsync());
            Assert.IsType<InjectedException>(exception);
        }

        private class InjectedException : Exception
        {
        }
    }
}
