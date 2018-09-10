using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
using Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils;
using Microsoft.Azure.Documents.Client;
using Moq;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    internal static class ChangeFeedDocumentClientExtensions
    {
        public static IChangeFeedDocumentClient SetupQueryResponse(this IChangeFeedDocumentClient client, string pid, string token, string documentLsn, string targetSession, Func<IFeedResponse<Document>, Task<IFeedResponse<Document>>> callback = null)
        {
            IList<Document> docs;
            if (documentLsn != null)
            {
                Document document = new Document();
                document.SetPropertyValue("_lsn", documentLsn);
                docs = new List<Document> { document };
            }
            else
            {
                docs = new List<Document>();
            }

            var feedResponse = Mock.Of<IFeedResponse<Document>>(f => 
                f.SessionToken == targetSession &&
                f.Count == docs.Count && 
                f.GetEnumerator() == docs.GetEnumerator());

            var documentQuery = new Mock<IChangeFeedDocumentQuery<Document>>();

            documentQuery
                .Setup(q => q.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()))
                .Returns(async (CancellationToken ct) =>
                {
                    var response = feedResponse;
                    if (callback != null)
                    {
                        response = await callback(response);
                    }
                    return response;
                });

            Mock.Get(client)
                .Setup(c => c.CreateDocumentChangeFeedQuery(
                    It.IsAny<string>(),
                    It.Is<ChangeFeedOptions>(o => o.PartitionKeyRangeId == pid && o.RequestContinuation == token)))
                .Returns(documentQuery.Object);

            return client;
        }

        public static IChangeFeedDocumentClient SetupQueryResponseFailure(
            this IChangeFeedDocumentClient client, 
            string partitionKeyRangeId, 
            string token, 
            Exception exception = null)
        {
            var documentQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>();
            Mock.Get(documentQuery)
                .Setup(q => q.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()))
                .ThrowsAsync(exception ?? DocumentExceptionHelpers.CreateNotFoundException());

            Mock.Get(client)
                .Setup(c => c.CreateDocumentChangeFeedQuery(
                    It.IsAny<string>(),
                    It.Is<ChangeFeedOptions>(o => o.PartitionKeyRangeId == partitionKeyRangeId && o.RequestContinuation == token)))
                .Returns(documentQuery);

            return client;
        }
    }
}