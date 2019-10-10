//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.FeedProcessor
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring;
    using Microsoft.Azure.Documents.Client;
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Moq;
    using Xunit;

    public class ChangeFeedQueryTimeoutDecoratorTests
    {
        [Fact]
        public async Task ExecuteNextAsync_WhenTimeout_NotifiesHealthMonitorAndThrows()
        {
            var monitor = Mock.Of<IHealthMonitor>();
            var innerQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>();

            var timeoutDecorator = new ChangeFeedQueryTimeoutDecorator(innerQuery, monitor, TimeSpan.FromMilliseconds(1));

            var feedResponse = Mock.Of<IFeedResponse<Document>>();

            Mock.Get(innerQuery)
                .Setup(q => q.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    await Task.Delay(TimeSpan.FromDays(1));
                    return feedResponse;
                });

            await Assert.ThrowsAsync<TimeoutException>(()=> timeoutDecorator.ExecuteNextAsync<Document>(CancellationToken.None));

            Mock.Get(monitor)
                .Verify(m => m.InspectAsync(It.Is<HealthMonitoringRecord>(record => record.Operation == MonitoredOperation.ChangeFeedQuery)), Times.Once);
        }

        [Fact]
        public async Task ExecuteNextAsync_WhenNoTimeout_ExecutesInnerQuery()
        {
            var monitor = Mock.Of<IHealthMonitor>();
            var innerQuery = Mock.Of<IChangeFeedDocumentQuery<Document>>();

            var timeoutDecorator = new ChangeFeedQueryTimeoutDecorator(innerQuery, monitor, TimeSpan.FromMinutes(10));

            var feedResponse = Mock.Of<IFeedResponse<Document>>();

            Mock.Get(innerQuery)
                .Setup(q => q.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()))
                .ReturnsAsync(feedResponse);

            await timeoutDecorator.ExecuteNextAsync<Document>(CancellationToken.None);

            Mock.Get(innerQuery)
                .Verify(q => q.ExecuteNextAsync<Document>(It.IsAny<CancellationToken>()), Times.Once);
        }
    }
}
