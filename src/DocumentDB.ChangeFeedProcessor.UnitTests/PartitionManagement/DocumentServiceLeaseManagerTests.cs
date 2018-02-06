//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    [Trait("Category", "Gated")]
    public class DocumentServiceLeaseManagerTests
    {
        private const string storeNamePrefix = "Name prefix";
        private const string collectionLink = "Collection link";
        private const string partitionId = "Partition ID";
        private const string continuationToken = "Continuation Token";
        private const string owner = "owner";
        private static readonly string leaseId = $"{storeNamePrefix}..{partitionId}";
        private static readonly DocumentCollectionInfo collectionInfo = new DocumentCollectionInfo()
        {
            DatabaseName = "DatabaseName",
            CollectionName = "CollectionName"
        };
        private static readonly Uri documentUri = UriFactory.CreateDocumentUri(collectionInfo.DatabaseName, collectionInfo.CollectionName, leaseId);

        class MockLease : ILease
        {
            public string PartitionId { get; set; }
            public string Owner { get; set; }
            public DateTime Timestamp { get; set; }
            public string ContinuationToken { get; set; }
            public string Id { get; set; }
            public string ConcurrencyToken { get; set; }
        }

        [Fact]
        public async Task ListLeasesAsync_ShouldReturnEmpty_WhenLeaseQueryReturnsNothing()
        {
            var queryMock = new Mock<IDocumentQuery<Document>>();
            var documentClient = Mock.Of<IDocumentClientEx>();
            SetupQuery(documentClient, queryMock);
            queryMock
                .Setup(q => q.HasMoreResults)
                .Returns(false);
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);

            var leases = (await leaseManager.ListLeasesAsync());
            Assert.Empty(leases);
        }

        [Fact]
        public async Task ListLeasesAsync_ShouldReturnLease_WhenLeaseQueryReturns()
        {
            var queryMock = new Mock<IDocumentQuery<Document>>();
            var documentClient = Mock.Of<IDocumentClientEx>();
            SetupQuery(documentClient, queryMock);
            queryMock
                .SetupSequence(q => q.HasMoreResults)
                .Returns(true)
                .Returns(false);
            var lease = new DocumentServiceLease();
            queryMock
                .Setup(q => q.ExecuteNextAsync<DocumentServiceLease>(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FeedResponse<DocumentServiceLease>(new[] { lease }));
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            ;

            var leases = (await leaseManager.ListLeasesAsync()).ToArray();

            Assert.Single(leases);
            Assert.Same(lease, leases[0]);
        }

        [Fact]
        public async Task ListLeasesAsync_ShouldReturnAllLeases_WhenMultiplePagesReturn()
        {
            var queryMock = new Mock<IDocumentQuery<Document>>();
            var documentClient = Mock.Of<IDocumentClientEx>();
            SetupQuery(documentClient, queryMock);
            queryMock
                .SetupSequence(q => q.HasMoreResults)
                .Returns(true)
                .Returns(true)
                .Returns(true)
                .Returns(false);
            var lease1 = new DocumentServiceLease();
            var lease2 = new DocumentServiceLease();
            queryMock
                .SetupSequence(q => q.ExecuteNextAsync<DocumentServiceLease>(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FeedResponse<DocumentServiceLease>(new[] { lease1 }))
                .ReturnsAsync(new FeedResponse<DocumentServiceLease>(new DocumentServiceLease[0]))
                .ReturnsAsync(new FeedResponse<DocumentServiceLease>(new[] { lease2 }));
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            ;

            var leases = (await leaseManager.ListLeasesAsync()).ToArray();

            Assert.Equal(2, leases.Length);
            Assert.Same(lease1, leases[0]);
            Assert.Same(lease2, leases[1]);
        }

        private static void SetupQuery(IDocumentClientEx documentClient, Mock<IDocumentQuery<Document>> queryMock)
        {
            Mock.Get(documentClient)
                .Setup(c => c.CreateDocumentQuery<Document>(collectionLink,
                    It.Is<SqlQuerySpec>(spec => spec.QueryText == "SELECT * FROM c WHERE STARTSWITH(c.id, @PartitionLeasePrefix)" &&
                                                spec.Parameters.Count == 1 &&
                                                spec.Parameters[0].Name == "@PartitionLeasePrefix" &&
                                                (string)spec.Parameters[0].Value == storeNamePrefix + ".."
                    )))
                .Returns(queryMock.As<IQueryable<Document>>().Object);
        }

        [Fact]
        public async Task CreateLeaseIfNotExistAsync_ReturnsLease_WhenDocumentCreated()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.CreateDocumentAsync(collectionLink, It.Is<DocumentServiceLease>(d =>
                    d.PartitionId == partitionId &&
                    d.ContinuationToken == null &&
                    d.Id == leaseId
                )))
                .ReturnsAsync(new ResourceResponse<Document>())
                .Verifiable();

            var lease = await leaseManager.CreateLeaseIfNotExistAsync(partitionId, null);

            Mock.Get(documentClient).VerifyAll();
            Assert.Equal(partitionId, lease.PartitionId);
            Assert.Null(lease.ContinuationToken);
            Assert.Null(lease.Owner);
            Assert.Equal(leaseId, lease.Id);
        }

        [Fact]
        public async Task CreateLeaseIfNotExistAsync_ReturnsNull_WhenDocumentExists()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.CreateDocumentAsync(collectionLink, It.Is<DocumentServiceLease>(d =>
                    d.PartitionId == partitionId &&
                    d.ContinuationToken == continuationToken &&
                    d.Id == $"{storeNamePrefix}..{partitionId}")))
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException())
                .Verifiable();

            var lease = await leaseManager.CreateLeaseIfNotExistAsync(partitionId, continuationToken);

            Mock.Get(documentClient).VerifyAll();
            Assert.Null(lease);
        }

        [Fact]
        public async Task CreateLeaseIfNotExistAsync_Throws_WhenCreateFails()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.CreateDocumentAsync(collectionLink, It.Is<DocumentServiceLease>(d =>
                    d.PartitionId == partitionId &&
                    d.ContinuationToken == continuationToken &&
                    d.Id == $"{storeNamePrefix}..{partitionId}")))
                .ThrowsAsync(DocumentExceptionHelpers.CreateNotFoundException());

            var exception = await Record.ExceptionAsync(async () => await leaseManager.CreateLeaseIfNotExistAsync(partitionId, continuationToken));

            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        [Fact]
        public async Task CheckpointAsync_UpdatesLease_WhenLeaseOwned()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            IDocumentServiceLeaseUpdater leaseUpdater = CreateLeaseUpdater(cachedLease);
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            const string newToken = "newToken";

            var lease = await leaseManager.CheckpointAsync(cachedLease, newToken);

            Assert.Equal(newToken, lease.ContinuationToken);
            Assert.Equal(leaseId, lease.Id);
        }

        [Fact]
        public async Task CheckpointAsync_ThrowsLeaseLost_WhenLeaseNotOwned()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease("new owner");
            IDocumentServiceLeaseUpdater leaseUpdater = CreateLeaseUpdater(cachedLease);
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            const string newToken = "newToken";

            var exception = await Record.ExceptionAsync(async () => await leaseManager.CheckpointAsync(cachedLease, newToken));

            Assert.IsAssignableFrom<LeaseLostException>(exception);
        }

        [Fact]
        public async Task AcquireAsync_UpdatesLease_WhenLeaseOwnershipDoesNotChange()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            IDocumentServiceLeaseUpdater leaseUpdater = CreateLeaseUpdater(cachedLease);
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            const string newOwner = "new owner";

            var lease = await leaseManager.AcquireAsync(cachedLease, newOwner);

            Assert.Equal(leaseId, lease.Id);
            Assert.Equal(newOwner, lease.Owner);
        }

        [Fact]
        public async Task AcquireAsync_ThrowsLeaseLost_WhenLeaseTakenByOtherOwner()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease("cached owner");
            IDocumentServiceLeaseUpdater leaseUpdater = CreateLeaseUpdater(cachedLease);
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);

            var exception = await Record.ExceptionAsync(async () => await leaseManager.AcquireAsync(cachedLease, "new owner"));

            Assert.IsAssignableFrom<LeaseLostException>(exception);
        }

        [Fact]
        public async Task RenewAsync_UpdatesLease_WhenOwningLease()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            const string storedContinuationToken = "A";

            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            MockLease storedLease = CreateStoredLease(owner);
            SetupUpdateLeaseAfterRead(leaseUpdater, storedContinuationToken, storedLease);
            SetupReadDocument(storedContinuationToken, documentClient);

            var lease = await leaseManager.RenewAsync(cachedLease);

            Assert.Equal(leaseId, lease.Id);
            Assert.Equal(owner, lease.Owner);
        }

        [Fact]
        public async Task RenewAsync_ThrowsLeaseLost_WhenDocumentIsNotFound()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.ReadDocumentAsync(It.IsAny<Uri>()))
                .ThrowsAsync(DocumentExceptionHelpers.CreateNotFoundException());

            var exception = await Record.ExceptionAsync(async () => await leaseManager.RenewAsync(cachedLease));

            Assert.IsAssignableFrom<LeaseLostException>(exception); ;
        }

        [Fact]
        public async Task RenewAsync_ReThrows_WhenOtherDocumentException()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.ReadDocumentAsync(It.IsAny<Uri>()))
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException());

            var exception = await Record.ExceptionAsync(async () => await leaseManager.RenewAsync(cachedLease));

            Assert.IsAssignableFrom<DocumentClientException>(exception); ;
        }

        [Fact]
        public async Task RenewAsync_ThrowsLeaseLost_WhenStoredLeaseOwnerDoesNotMatch()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            const string storedContinuationToken = "A";

            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            MockLease storedLease = CreateStoredLease("different owner");
            SetupUpdateLeaseAfterRead(leaseUpdater, storedContinuationToken, storedLease);
            SetupReadDocument(storedContinuationToken, documentClient);

            var exception = await Record.ExceptionAsync(async () => await leaseManager.RenewAsync(cachedLease));

            Assert.IsAssignableFrom<LeaseLostException>(exception); ;
        }

        [Fact]
        public async Task ReleaseAsync_SetsLeasOwnerToNull_WhenOwningLease()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            const string storedContinuationToken = "A";

            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            MockLease storedLease = CreateStoredLease(owner);
            SetupUpdateLeaseAfterRead(leaseUpdater, storedContinuationToken, storedLease);
            SetupReadDocument(storedContinuationToken, documentClient);

            await leaseManager.ReleaseAsync(cachedLease);

            Assert.Null(storedLease.Owner);
        }

        [Fact]
        public async Task ReleaseAsync_ThrowsLeaseLost_WhenDocumentIsNotFound()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.ReadDocumentAsync(It.IsAny<Uri>()))
                .ThrowsAsync(DocumentExceptionHelpers.CreateNotFoundException());

            var exception = await Record.ExceptionAsync(async () => await leaseManager.ReleaseAsync(cachedLease));

            Assert.IsAssignableFrom<LeaseLostException>(exception); ;
        }

        [Fact]
        public async Task ReleaseAsync_ReThrows_WhenOtherDocumentException()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.ReadDocumentAsync(It.IsAny<Uri>()))
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException());

            var exception = await Record.ExceptionAsync(async () => await leaseManager.ReleaseAsync(cachedLease));

            Assert.IsAssignableFrom<DocumentClientException>(exception); ;
        }

        [Fact]
        public async Task ReleaseAsync_ThrowsLeaseLost_WhenStoredLeaseOwnerDoesNotMatch()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);
            const string storedContinuationToken = "A";

            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            MockLease storedLease = CreateStoredLease("different owner");
            SetupUpdateLeaseAfterRead(leaseUpdater, storedContinuationToken, storedLease);
            SetupReadDocument(storedContinuationToken, documentClient);

            var exception = await Record.ExceptionAsync(async () => await leaseManager.ReleaseAsync(cachedLease));

            Assert.IsAssignableFrom<LeaseLostException>(exception); ;
        }

        [Fact]
        public async Task DeleteAsync_DeletesDocument_IfExists()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);

            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.DeleteDocumentAsync(documentUri))
                .ReturnsAsync(new ResourceResponse<Document>(new Document()))
                .Verifiable();

            await leaseManager.DeleteAsync(cachedLease);

            Mock.Get(documentClient).VerifyAll();
        }

        [Fact]
        public async Task DeleteAsync_ReturnsSuccess_IfDocumentDoesNotExist()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);

            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.DeleteDocumentAsync(documentUri))
                .ThrowsAsync(DocumentExceptionHelpers.CreateNotFoundException())
                .Verifiable();

            await leaseManager.DeleteAsync(cachedLease);

            Mock.Get(documentClient).VerifyAll();
        }

        [Fact]
        public async Task DeleteAsync_Throws_IfDeleteFails()
        {
            var documentClient = Mock.Of<IDocumentClientEx>();
            var cachedLease = CreateCachedLease(owner);

            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            var leaseManager = CreateLeaseManager(documentClient, leaseUpdater);
            Mock.Get(documentClient)
                .Setup(c => c.DeleteDocumentAsync(documentUri))
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException())
                .Verifiable();

            var exception = await Record.ExceptionAsync(async () => await leaseManager.DeleteAsync(cachedLease));

            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        private static void SetupReadDocument(string storedContinuationToken, IDocumentClientEx documentClient)
        {
            Document storedLeaseDocument = new Document();
            storedLeaseDocument.SetPropertyValue("id", leaseId);
            storedLeaseDocument.SetPropertyValue("PartitionId", partitionId);
            storedLeaseDocument.SetPropertyValue("ContinuationToken", storedContinuationToken);
            Mock.Get(documentClient)
                .Setup(c => c.ReadDocumentAsync(documentUri))
                .ReturnsAsync(new ResourceResponse<Document>(storedLeaseDocument));
        }

        private static void SetupUpdateLeaseAfterRead(IDocumentServiceLeaseUpdater leaseUpdater, string storedContinuationToken, MockLease storedLease)
        {
            Mock.Get(leaseUpdater)
                .Setup(u => u.UpdateLeaseAsync(It.Is<ILease>(l => l.ContinuationToken == storedContinuationToken), documentUri, It.IsAny<Func<ILease, ILease>>()))
                .Callback((ILease l, Uri uri, Func<ILease, ILease> callback) => callback(storedLease))
                .ReturnsAsync(storedLease);
        }

        private DocumentServiceLeaseManager CreateLeaseManager(IDocumentClientEx documentClient, IDocumentServiceLeaseUpdater leaseUpdater)
        {
            return new DocumentServiceLeaseManager(documentClient, leaseUpdater, collectionInfo, storeNamePrefix, collectionLink);
        }

        private IDocumentServiceLeaseUpdater CreateLeaseUpdater(ILease expectedCachedLease)
        {
            var leaseUpdater = Mock.Of<IDocumentServiceLeaseUpdater>();
            MockLease storedLease = CreateStoredLease(owner);
            Mock.Get(leaseUpdater)
                .Setup(u => u.UpdateLeaseAsync(expectedCachedLease, documentUri, It.IsAny<Func<ILease, ILease>>()))
                .Callback((ILease cachedLease, Uri uri, Func<ILease, ILease> callback) => callback(storedLease))
                .ReturnsAsync(storedLease);
            return leaseUpdater;
        }

        private static MockLease CreateStoredLease(string leaseOwner)
        {
            return new MockLease()
            {
                Id = leaseId,
                Owner = leaseOwner
            };
        }

        private static ILease CreateCachedLease(string leaseOwner)
        {
            return new MockLease()
            {
                Id = leaseId,
                PartitionId = partitionId,
                ContinuationToken = "oldToken",
                Owner = leaseOwner
            };
        }
    }
}