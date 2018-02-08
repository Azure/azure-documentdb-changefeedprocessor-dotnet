//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests
{
    public class ChangeFeedEventHostInternal: ChangeFeedEventHost
    {
        private readonly ChangeFeedHostBuilder builder = new ChangeFeedHostBuilder();

        public ChangeFeedEventHostInternal(string hostName,
                                   DocumentCollectionInfo feedCollectionLocation,
                                   DocumentCollectionInfo leaseCollectionLocation,
                                   IDocumentClientEx feedCollectionClient,
                                   IDocumentClientEx leaseCollectionClient,
                                   ILeaseManager leaseManager,
                                   ChangeFeedHostOptions changeFeedHostOptions): base(hostName, feedCollectionLocation, leaseCollectionLocation)
        {
            this.builder
                .WithHostName(hostName)
                .WithFeedDocumentClient(feedCollectionClient)
                .WithFeedCollection(feedCollectionLocation)
                .WithChangeFeedHostOptions(changeFeedHostOptions)
                .WithLeaseManager(leaseManager)
                .WithLeaseCollection(leaseCollectionLocation)
                .WithLeaseDocumentClient(leaseCollectionClient);
        }
    }

    [Trait("Category", "Gated")]
    public class ChangeFeedEventHostTests
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

        private readonly ChangeFeedEventHost changeFeedEventHost;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly IChangeFeedObserver observer;
        private readonly IChangeFeedObserverFactory observerFactory;

        public ChangeFeedEventHostTests()
        {
            var leaseQueryMock = new Mock<IDocumentQuery<Document>>();
            var leaseDocumentClient = Mock.Of<IDocumentClientEx>();
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
            var documentQuery = Mock.Of<IDocumentQueryEx<Document>>();
            Mock.Get(documentQuery)
                .Setup(query => query.HasMoreResults)
                .Returns(false);
            Mock.Get(documentQuery)
                .Setup(query => query.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => feedResponse)
                .Callback(() => cancellationTokenSource.Cancel());
            var documentClient = Mock.Of<IDocumentClientEx>();
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
                .Setup(manager => manager.AcquireAsync(lease, "host"))
                .ReturnsAsync(lease);

            Mock.Get(leaseManager)
                .Setup(manager => manager.ReleaseAsync(lease))
                .Returns(Task.FromResult(false));

            this.changeFeedEventHost = new ChangeFeedEventHostInternal("someHost", collectionInfo, collectionInfo, documentClient, leaseDocumentClient, leaseManager, new ChangeFeedHostOptions());

            this.observer = Mock.Of<IChangeFeedObserver>();
            Mock.Get(observer)
                .Setup(feedObserver => feedObserver
                    .ProcessChangesAsync(It.IsAny<ChangeFeedObserverContext>(), It.IsAny<IReadOnlyList<Document>>()))
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
        public async Task RegisterObserver_Then_Estimate()
        {
            await this.changeFeedEventHost.RegisterObserverFactoryAsync(this.observerFactory);
            await this.changeFeedEventHost.GetEstimatedRemainingWork();
        }

        [Fact]
        public async Task Estimate_Then_RegisterObserver()
        {
            await this.changeFeedEventHost.GetEstimatedRemainingWork();
            await this.changeFeedEventHost.RegisterObserverFactoryAsync(this.observerFactory);
        }
    }
}
