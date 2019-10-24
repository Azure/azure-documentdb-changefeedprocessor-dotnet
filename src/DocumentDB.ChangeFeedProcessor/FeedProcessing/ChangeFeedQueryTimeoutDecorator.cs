// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
    using Microsoft.Azure.Documents.Client;

    internal class ChangeFeedQueryTimeoutDecorator : IChangeFeedDocumentQuery<Document>
    {
        private readonly IChangeFeedDocumentQuery<Document> query;
        private readonly IHealthMonitor monitor;
        private readonly TimeSpan timeout;

        public ChangeFeedQueryTimeoutDecorator(IChangeFeedDocumentQuery<Document> query, IHealthMonitor monitor, TimeSpan timeout)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));
            if (monitor == null)
                throw new ArgumentNullException(nameof(monitor));

            this.query = query;
            this.monitor = monitor;
            this.timeout = timeout;
        }

        public bool HasMoreResults => this.query.HasMoreResults;

        public async Task<IFeedResponse<TResult>> ExecuteNextAsync<TResult>(CancellationToken token = default(CancellationToken))
        {
            try
            {
                return await this.query.ExecuteNextAsync<TResult>(token).AbortOnTimeout(this.timeout, token).ConfigureAwait(false);
            }
            catch (TimeoutException e)
            {
                await this.monitor
                    .InspectAsync(new HealthMonitoringRecord(HealthSeverity.Error, MonitoredOperation.ReadChangeFeed, null, e)).ConfigureAwait(false);
                throw;
            }
        }

        public void Dispose()
        {
            this.query.Dispose();
        }
    }
}
