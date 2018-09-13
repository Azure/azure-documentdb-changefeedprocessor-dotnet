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
        public Task InspectAsync(HealthMonitoringRecord record)
        {
            if (record.Severity == HealthSeverity.Error)
            {
                Logger.ErrorException($"Unhealthiness detected in the operation {record.Operation} for {record.Lease}. ", record.Exception);
            }

            return Task.FromResult(true);
        }
    }
}