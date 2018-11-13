//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
using Microsoft.Azure.Documents.Client;
using Moq;
using Newtonsoft.Json;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.LeaseManagement
{
    [Trait("Category", "Gated")]
    public class DocumentServiceLeaseStoreTests
    {
        private static readonly DocumentCollectionInfo collectionInfo = new DocumentCollectionInfo()
        {
            DatabaseName = "DatabaseName",
            CollectionName = "CollectionName"
        };

        private readonly TimeSpan lockTime = TimeSpan.FromMilliseconds(100);
        private readonly string leaseCollectionLink = "leaseStore";
        private const string containerNamePrefix = "prefix";
        private const string storeMarker = containerNamePrefix + ".info";

        [Fact]
        public async Task IsInitializedAsync_ShouldReturnTrue_IfDocumentExist()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.ReadDocumentAsync(It.Is<Uri>(uri => uri.ToString().EndsWith(storeMarker)), null, default(CancellationToken)))
                .ReturnsAsync(CreateResponse());

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            bool isInited = await leaseStore.IsInitializedAsync();
            Assert.True(isInited);
        }

        [Fact]
        public async Task IsInitializedAsync_ShouldReturnFalse_IfNoDocument()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.ReadDocumentAsync(It.Is<Uri>(uri => uri.ToString().EndsWith(storeMarker)), null, default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateNotFoundException());

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            bool isInited = await leaseStore.IsInitializedAsync();
            Assert.False(isInited);
        }

        [Fact]
        public async Task AcquireInitializationLockAsync_ShouldReturnTrue_IfLockSucceeds()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(leaseCollectionLink, It.IsAny<object>(), null, false, default(CancellationToken)))
                .ReturnsAsync(new ResourceResponse<Document>(new Document()));

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            bool isLocked = await leaseStore.AcquireInitializationLockAsync(lockTime);
            Assert.True(isLocked);

            Mock.Get(client)
                .Verify(c =>
                        c.CreateDocumentAsync(leaseCollectionLink, It.Is<Document>(d => d.TimeToLive == (int)lockTime.TotalSeconds && d.Id == "prefix.lock"),
                        null,
                        false,
                        default(CancellationToken)),
                    Times.Once);
        }

        [Fact]
        public async Task AcquireInitializationLockAsync_ShouldReturnFalse_IfLockConflicts()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(It.IsAny<string>(), It.IsAny<object>(), null, false, default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException());

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            bool isLocked = await leaseStore.AcquireInitializationLockAsync(lockTime);
            Assert.False(isLocked);

            Mock.Get(client)
                .Verify(c =>
                        c.CreateDocumentAsync(leaseCollectionLink, It.Is<Document>(d => d.TimeToLive == (int)lockTime.TotalSeconds && d.Id == "prefix.lock"),
                        null,
                        false,
                        default(CancellationToken)),
                    Times.Once);
        }

        [Fact]
        public async Task AcquireInitializationLockAsync_ShouldThrow_IfLockThrows()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(It.IsAny<string>(), It.IsAny<object>(), null, false, default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateRequestRateTooLargeException());

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            Exception exception = await Record.ExceptionAsync(() => leaseStore.AcquireInitializationLockAsync(lockTime));
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        [Fact]
        public async Task ReleaseInitializationLockAsync_ShouldReturnTrue_IfLockDeleted()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.DeleteDocumentAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>(), default(CancellationToken)))
                .ReturnsAsync(new ResourceResponse<Document>(new Document()));

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            bool isLockFoundAndReleased = await leaseStore.ReleaseInitializationLockAsync();
            Assert.True(isLockFoundAndReleased);

            Mock.Get(client)
                .Verify(c => c.DeleteDocumentAsync(It.Is<Uri>(uri => uri.OriginalString.EndsWith("prefix.lock")), It.IsAny<RequestOptions>(), default(CancellationToken)), Times.Once);
        }

        [Fact]
        public async Task ReleaseInitializationLockAsync_ShouldReturnFalse_IfLockNotFound()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.DeleteDocumentAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>(), default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateNotFoundException());

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            bool isLockFoundAndReleased = await leaseStore.ReleaseInitializationLockAsync();
            Assert.False(isLockFoundAndReleased);

            Mock.Get(client)
                .Verify(c => c.DeleteDocumentAsync(It.Is<Uri>(uri => uri.OriginalString.EndsWith("prefix.lock")), It.IsAny<RequestOptions>(), default(CancellationToken)), Times.Once);
        }

        [Fact]
        public async Task ReleaseInitializationLockAsync_ShouldThrow_IfLockThrows()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.DeleteDocumentAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>(), default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateRequestRateTooLargeException());

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            Exception exception = await Record.ExceptionAsync(() => leaseStore.ReleaseInitializationLockAsync());
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        [Fact]
        public async Task ReleaseInitializationLockAsync_PassesOverETag()
        {
            var etag = "etag";

            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(leaseCollectionLink, It.IsAny<object>(), null, false, default(CancellationToken)))
                .ReturnsAsync(new ResourceResponse<Document>(JsonConvert.DeserializeObject<Document>($"{{\"_etag\":\"{etag}\"}}")));
            Mock.Get(client)
                .Setup(c => c.DeleteDocumentAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>(), default(CancellationToken)))
                .ReturnsAsync(new ResourceResponse<Document>(new Document()));

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());

            bool isLockAcquired = await leaseStore.AcquireInitializationLockAsync(lockTime);
            Assert.True(isLockAcquired);

            bool isLockFoundAndReleased = await leaseStore.ReleaseInitializationLockAsync();
            Assert.True(isLockFoundAndReleased);

            Mock.Get(client)
                .Verify(c => c.DeleteDocumentAsync(
                    It.IsAny<Uri>(),
                    It.Is<RequestOptions>(options => options.AccessCondition.Type == AccessConditionType.IfMatch && options.AccessCondition.Condition == etag),
                    default(CancellationToken)),
                    Times.Once);
        }

        [Fact]
        public async Task ReleaseInitializationLockAsync_ShouldPassPartitionKey_IfLeaseCollectionPartitioned()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.DeleteDocumentAsync(It.IsAny<Uri>(), It.IsAny<RequestOptions>(), default(CancellationToken)))
                .ReturnsAsync(new ResourceResponse<Document>(new Document()));

            var pkValue = "pk";
            var requestOptionsFactory = Mock.Of<IRequestOptionsFactory>();
            Mock.Get(requestOptionsFactory)
                .Setup(factory => factory.CreateRequestOptions(It.IsAny<ILease>()))
                .Returns(new RequestOptions { PartitionKey = new PartitionKey(pkValue)});

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, requestOptionsFactory);
            bool isLockFoundAndReleased = await leaseStore.ReleaseInitializationLockAsync();
            Assert.True(isLockFoundAndReleased);

            Mock.Get(client)
                .Verify(c => c.DeleteDocumentAsync(
                    It.Is<Uri>(uri => uri.OriginalString.EndsWith("prefix.lock")),
                    It.Is<RequestOptions>(options => new PartitionKey(pkValue).Equals(options.PartitionKey)),
                    default(CancellationToken)));
        }

        [Fact]
        public async Task MarkInitializedAsync_ShouldSucceed_IfMarkerCreated()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(leaseCollectionLink, It.IsAny<object>(), null, false, default(CancellationToken)))
                .ReturnsAsync(new ResourceResponse<Document>(new Document()));

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            await leaseStore.MarkInitializedAsync();

            Mock.Get(client)
                .Verify(c =>
                        c.CreateDocumentAsync(leaseCollectionLink, It.Is<Document>(d => d.Id == storeMarker),
                        null,
                        false,
                        default(CancellationToken)),
                    Times.Once);
        }

        [Fact]
        public async Task MarkInitializedAsync_ShouldSucceed_IfMarkerConflicts()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(It.IsAny<string>(), It.IsAny<object>(), null, false, default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException());

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            await leaseStore.MarkInitializedAsync();

            Mock.Get(client)
                .Verify(c =>
                        c.CreateDocumentAsync(leaseCollectionLink, It.Is<Document>(d => d.Id == storeMarker),
                        null,
                        false,
                        default(CancellationToken)),
                    Times.Once);
        }

        [Fact]
        public async Task MarkInitializedAsync_ShouldThrow_IfMarkerThrows()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(It.IsAny<string>(), It.IsAny<object>(), null, false, default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateRequestRateTooLargeException());

            var leaseStore = new DocumentServiceLeaseStore(client, collectionInfo, containerNamePrefix, leaseCollectionLink, Mock.Of<IRequestOptionsFactory>());
            Exception exception = await Record.ExceptionAsync(async () => await leaseStore.MarkInitializedAsync());
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        private ResourceResponse<Document> CreateResponse()
        {
            return new ResourceResponse<Document>(new Document());
        }
    }
}