//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring
{
    using System;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// A record used in the health monitoring.
    /// </summary>
    public class HealthMonitoringRecord
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="HealthMonitoringRecord"/> class.
        /// </summary>
        /// <param name="severity">The health severity level.</param>
        /// <param name="operation">The operation.</param>
        /// <param name="lease">The lease.</param>
        /// <param name="exception">The exception.</param>
        public HealthMonitoringRecord(HealthSeverity severity, MonitoredOperation operation, ILease lease, Exception exception)
        {
            if (lease == null) throw new ArgumentNullException(nameof(lease));
            this.Severity = severity;
            this.Operation = operation;
            this.Lease = lease;
            this.Exception = exception;
        }

        /// <summary>
        /// Gets the health severity.
        /// </summary>
        public HealthSeverity Severity { get; }

        /// <summary>
        /// Gets the monitored operation.
        /// </summary>
        public MonitoredOperation Operation { get; }

        /// <summary>
        /// Gets the lease which triggered the operation.
        /// </summary>
        public ILease Lease { get; }

        /// <summary>
        /// Gets the exception details in case of failure.
        /// </summary>
        public Exception Exception { get; }
    }
}