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
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
    using Microsoft.Azure.Documents.Client;

    internal class ChangeFeedQueryTimeoutDecorator : IChangeFeedDocumentQuery<Document>
    {
        private readonly IChangeFeedDocumentQuery<Document> query;
        private readonly IHealthMonitor monitor;
        private readonly ILease lease;
        private readonly TimeSpan timeout;

        public ChangeFeedQueryTimeoutDecorator(IChangeFeedDocumentQuery<Document> query, IHealthMonitor monitor, TimeSpan timeout, ILease lease)
        {
            this.query = query;
            this.monitor = monitor;
            this.timeout = timeout;
            this.lease = lease;
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
                    .InspectAsync(new HealthMonitoringRecord(HealthSeverity.Error, MonitoredOperation.ReadChangeFeed, this.lease, e)).ConfigureAwait(false);
                throw;
            }
        }

        public void Dispose()
        {
            this.query.Dispose();
        }
    }
}
