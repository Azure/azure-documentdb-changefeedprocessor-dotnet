//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
using Microsoft.Azure.Documents.Client;
using Moq;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    [Trait("Category", "Gated")]
    public class LeaseStoreTests
    {
        private static readonly DocumentCollectionInfo collectionInfo = new DocumentCollectionInfo()
        {
            DatabaseName = "DatabaseName",
            CollectionName = "CollectionName"
        };

        private readonly TimeSpan lockTime = TimeSpan.FromMilliseconds(100);
        private readonly CollectionMetadata leaseStoreCollectionProperties = new CollectionMetadata("leaseStore", false);
        private const string containerNamePrefix = "prefix";
        private const string storeMarker = containerNamePrefix + ".info";

        [Fact]
        public async Task IsInitializedAsync_ShouldReturnTrue_IfDocumentExist()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.ReadDocumentAsync(It.Is<Uri>(uri => uri.ToString().EndsWith(storeMarker)), null, default(CancellationToken)))
                .ReturnsAsync(CreateResponse());

            var leaseStore = new LeaseStore(client, collectionInfo, containerNamePrefix, leaseStoreCollectionProperties);
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

            var leaseStore = new LeaseStore(client, collectionInfo, containerNamePrefix, leaseStoreCollectionProperties);
            bool isInited = await leaseStore.IsInitializedAsync();
            Assert.False(isInited);
        }

        [Fact]
        public async Task LockInitializationAsync_ShouldReturnTrue_IfLockSucceeds()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            var leaseStore = new LeaseStore(client, collectionInfo, containerNamePrefix, leaseStoreCollectionProperties);
            bool isLocked = await leaseStore.LockInitializationAsync(lockTime);
            Assert.True(isLocked);

            Mock.Get(client)
                .Verify(c =>
                        c.CreateDocumentAsync(leaseStoreCollectionProperties.SelfLink, It.Is<Document>(d => d.TimeToLive == (int)lockTime.TotalSeconds && d.Id == "prefix.lock"),
                        null,
                        false,
                        default(CancellationToken)),
                    Times.Once);
        }

        [Fact]
        public async Task LockInitializationAsync_ShouldReturnFalse_IfLockConflicts()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(It.IsAny<string>(), It.IsAny<object>(), null, false, default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateConflictException());

            var leaseStore = new LeaseStore(client, collectionInfo, containerNamePrefix, leaseStoreCollectionProperties);
            bool isLocked = await leaseStore.LockInitializationAsync(lockTime);
            Assert.False(isLocked);

            Mock.Get(client)
                .Verify(c =>
                        c.CreateDocumentAsync(leaseStoreCollectionProperties.SelfLink, It.Is<Document>(d => d.TimeToLive == (int)lockTime.TotalSeconds && d.Id == "prefix.lock"),
                        null,
                        false,
                        default(CancellationToken)),
                    Times.Once);
        }

        [Fact]
        public async Task LockInitializationAsync_ShouldThrow_IfLockThrows()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.CreateDocumentAsync(It.IsAny<string>(), It.IsAny<object>(), null, false, default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreateRequestRateTooLargeException());

            var leaseStore = new LeaseStore(client, collectionInfo, containerNamePrefix, leaseStoreCollectionProperties);
            Exception exception = await Record.ExceptionAsync(() => leaseStore.LockInitializationAsync(lockTime));
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        [Fact]
        public async Task MarkInitializedAsync_ShouldSucceed_IfMarkerCreated()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            var leaseStore = new LeaseStore(client, collectionInfo, containerNamePrefix, leaseStoreCollectionProperties);
            await leaseStore.MarkInitializedAsync();

            Mock.Get(client)
                .Verify(c =>
                        c.CreateDocumentAsync(leaseStoreCollectionProperties.SelfLink, It.Is<Document>(d => d.Id == storeMarker),
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

            var leaseStore = new LeaseStore(client, collectionInfo, containerNamePrefix, leaseStoreCollectionProperties);
            await leaseStore.MarkInitializedAsync();

            Mock.Get(client)
                .Verify(c =>
                        c.CreateDocumentAsync(leaseStoreCollectionProperties.SelfLink, It.Is<Document>(d => d.Id == storeMarker),
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

            var leaseStore = new LeaseStore(client, collectionInfo, containerNamePrefix, leaseStoreCollectionProperties);
            Exception exception = await Record.ExceptionAsync(async () => await leaseStore.MarkInitializedAsync());
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        private ResourceResponse<Document> CreateResponse()
        {
            return new ResourceResponse<Document>(new Document());
        }
    }
}