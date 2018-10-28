//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
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
    public class DocumentServiceLeaseUpdaterTests
    {
        private const string eTag1 = "ETag #1";
        private const string eTag2 = "ETag #2";
        private const string eTag3 = "ETag #3";
        private readonly Uri documentUri = new Uri("https://a.com/x");

        [Fact]
        public async Task UpdateLeaseAsync_ShouldReturnNull_IfMergerReturnsNull()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            var updater = new DocumentServiceLeaseUpdater(client);
            var lease = Mock.Of<ILease>();

            var newLease = await updater.UpdateLeaseAsync(lease, documentUri, null, serverLease => null);

            Assert.Null(newLease);
        }

        [Fact]
        public async Task UpdateLeaseAsync_ShouldReturnNewLease_WhenReplaceSucceeds()
        {
            ILease oldLease = CreateLease();
            ILease updatedLease = CreateLease(eTag1);

            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.ReplaceDocumentAsync(
                    documentUri,
                    updatedLease,
                    It.Is<RequestOptions>(options => options.AccessCondition.Type == AccessConditionType.IfMatch && options.AccessCondition.Condition == eTag1),
                    default(CancellationToken)))
                .ReturnsAsync(CreateLeaseResponse(eTag2));
            var updater = new DocumentServiceLeaseUpdater(client);

            var newLease = await updater.UpdateLeaseAsync(oldLease, documentUri, null, serverLease => updatedLease);

            Assert.Equal(eTag2, newLease.ConcurrencyToken);
        }

        [Fact]
        public async Task UpdateLeaseAsync_ShouldThrowLostLeaseException_WhenReplaceReturnsNotFound()
        {
            Exception exception = await TestReplaceException(DocumentExceptionHelpers.CreateNotFoundException());
            Assert.IsAssignableFrom<LeaseLostException>(exception);
        }

        [Fact]
        public async Task UpdateLeaseAsync_ShouldThrowLostLeaseException_WhenReplaceReturnsConflict()
        {
            Exception exception = await TestReplaceException(DocumentExceptionHelpers.CreateConflictException());
            Assert.IsAssignableFrom<LeaseLostException>(exception);
        }

        [Fact]
        public async Task UpdateLeaseAsync_ShouldRethrow_WhenReplaceReturnsOtherError()
        {
            Exception exception = await TestReplaceException(DocumentExceptionHelpers.CreateException("Microsoft.Azure.Documents.GoneException", 1));
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }


        private async Task<Exception> TestReplaceException(Exception replaceException)
        {
            ILease oldLease = CreateLease();
            ILease updatedLease = CreateLease(eTag1);

            var client = Mock.Of<IChangeFeedDocumentClient>();
            Mock.Get(client)
                .Setup(c => c.ReplaceDocumentAsync(
                    documentUri,
                    updatedLease,
                    It.Is<RequestOptions>(options => options.AccessCondition.Type == AccessConditionType.IfMatch && options.AccessCondition.Condition == eTag1),
                    default(CancellationToken)))
                .ThrowsAsync(replaceException);
            var updater = new DocumentServiceLeaseUpdater(client);

            return await Record.ExceptionAsync(async () => await updater.UpdateLeaseAsync(oldLease, documentUri, null, serverLease => updatedLease));
        }

        [Fact]
        public async Task UpdateLeaseAsync_ShouldThrowLostLeaseException_WhenConflictAndReadReturnsNotFound()
        {
            Exception exception = await TestReadException(DocumentExceptionHelpers.CreateNotFoundException());
            Assert.IsAssignableFrom<LeaseLostException>(exception);
        }

        [Fact]
        public async Task UpdateLeaseAsync_ShouldRethrow_WhenConflictAndReadReturnsOtherException()
        {
            Exception exception = await TestReadException(DocumentExceptionHelpers.CreatePreconditionFailedException());
            Assert.IsAssignableFrom<DocumentClientException>(exception);
        }

        private async Task<Exception> TestReadException(Exception readException)
        {
            ILease oldLease = CreateLease();
            ILease updatedLease = CreateLease(eTag1);

            var client = Mock.Of<IChangeFeedDocumentClient>();
            SetupReplaceConflict(client, updatedLease);
            Mock.Get(client)
                .Setup(c => c.ReadDocumentAsync(documentUri, null, default(CancellationToken)))
                .ThrowsAsync(readException).Verifiable();

            var updater = new DocumentServiceLeaseUpdater(client);
            Exception exception = await Record.ExceptionAsync(async () => await updater.UpdateLeaseAsync(oldLease, documentUri, null, serverLease => updatedLease));
            Mock.Get(client).VerifyAll();
            return exception;
        }

        [Fact]
        public async Task UpdateLeaseAsync_ShouldRetryWithUpdatedLease_WhenConflict()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            var updater = new DocumentServiceLeaseUpdater(client);
            ILease oldLease = CreateLease();

            // setup pass #1
            ILease updatedLease1 = CreateLease(eTag1);
            SetupReplaceConflict(client, updatedLease1);
            Mock.Get(client)
                .Setup(c => c.ReadDocumentAsync(documentUri, null, default(CancellationToken)))
                .ReturnsAsync(CreateLeaseResponse(eTag2));

            // setup pass #2
            ILease updatedLease2 = CreateLease(eTag2);
            Mock.Get(client)
                .Setup(c => c.ReplaceDocumentAsync(
                    documentUri,
                    updatedLease2,
                    It.Is<RequestOptions>(options => options.AccessCondition.Type == AccessConditionType.IfMatch && options.AccessCondition.Condition == eTag2),
                    default(CancellationToken)))
                .ReturnsAsync(CreateLeaseResponse(eTag3));

            ILease lease =  await updater.UpdateLeaseAsync(oldLease, documentUri, null, serverLease =>
            {
                if (serverLease.ConcurrencyToken == null) return updatedLease1;
                if (serverLease.ConcurrencyToken == eTag2) return updatedLease2;
                throw new InvalidOperationException();
            });

            Assert.Equal(eTag3, lease.ConcurrencyToken);
        }

        [Fact]
        public async Task UpdateLeaseAsync_ShouldThrowLeaseLostException_WhenConflictAfterAllRetries()
        {
            var client = Mock.Of<IChangeFeedDocumentClient>();
            var updater = new DocumentServiceLeaseUpdater(client);
            ILease oldLease = CreateLease();
            const int retryCount = 5;
            var getDocumentSequence = Mock.Get(client)
                .SetupSequence(c => c.ReadDocumentAsync(documentUri, null, default(CancellationToken)));
            for (int i = 0; i <= retryCount; i++)
            {
                string eTag = i.ToString();
                Mock.Get(client)
                    .Setup(c => c.ReplaceDocumentAsync(
                        documentUri,
                        It.Is<ILease>(lease => lease.ConcurrencyToken == eTag),
                        It.Is<RequestOptions>(options => options.AccessCondition.Type == AccessConditionType.IfMatch && options.AccessCondition.Condition == eTag),
                        default(CancellationToken)))
                    .ThrowsAsync(DocumentExceptionHelpers.CreatePreconditionFailedException());
                getDocumentSequence = getDocumentSequence.ReturnsAsync(CreateLeaseResponse(eTag));
            }

            int callbackInvokeCount = 0;
            Exception exception = await Record.ExceptionAsync(async () => await updater.UpdateLeaseAsync(oldLease, documentUri, null, serverLease =>
            {
                callbackInvokeCount++;
                if (serverLease.ConcurrencyToken == null) return CreateLease("0");
                return CreateLease((int.Parse(serverLease.ConcurrencyToken) + 1).ToString());
            }));

            Assert.IsAssignableFrom<LeaseLostException>(exception);
            Assert.Equal(retryCount+1, callbackInvokeCount);
        }


        private void SetupReplaceConflict(IChangeFeedDocumentClient client, ILease updatedLease)
        {
            Mock.Get(client)
                .Setup(c => c.ReplaceDocumentAsync(
                    documentUri,
                    updatedLease,
                    It.Is<RequestOptions>(options => options.AccessCondition.Type == AccessConditionType.IfMatch && options.AccessCondition.Condition == eTag1),
                    default(CancellationToken)))
                .ThrowsAsync(DocumentExceptionHelpers.CreatePreconditionFailedException());
        }

        private static ILease CreateLease(string eTag = null)
        {
            var updatedLease = Mock.Of<ILease>();
            Mock.Get(updatedLease)
                .SetupGet(l => l.ConcurrencyToken)
                .Returns(eTag);
            return updatedLease;
        }

        private ResourceResponse<Document> CreateLeaseResponse(string eTag)
        {
            var lease = new DocumentServiceLease()
            {
                ETag = eTag
            };
            string json = JsonConvert.SerializeObject(lease);
            Document document = JsonConvert.DeserializeObject<Document>(json);
            return new ResourceResponse<Document>(document);
        }
    }
}
