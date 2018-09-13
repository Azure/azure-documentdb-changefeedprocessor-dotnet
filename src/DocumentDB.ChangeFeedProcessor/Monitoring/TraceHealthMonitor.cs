//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// A monitor which logs the errors only.
    /// </summary>
    internal class TraceHealthMonitor : IHealthMonitor
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        /// <inheritdoc />
        public Task InspectAsync(HealthSeverity severity, MonitoredOperation operation, ILease lease, Exception exception = null)
        {
            if (severity == HealthSeverity.Error)
            {
                Logger.ErrorException($"Unhealthiness detected in the operation {operation} for {lease}. ", exception);
            }

            return Task.FromResult(true);
        }
    }
}