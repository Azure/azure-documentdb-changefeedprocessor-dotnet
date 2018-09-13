//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// A strategy for handling the situation when the change feed processor is not able to aquire lease due to unknown reasons.
    /// </summary>
    public interface IHealthMonitor
    {
        /// <summary>
        /// A logic to handle that exceptional situation.
        /// </summary>
        /// <param name="severity">The health severity.</param>
        /// <param name="operation">The operation which reports healthiness or failure.</param>
        /// <param name="lease">Last lease which the system tried to aquire but failed.</param>
        /// <param name="exception">Last unknonw exception thrown when the system tried to aquire the lease.</param>
        Task InspectAsync(HealthSeverity severity, MonitoredOperation operation, ILease lease, Exception exception = null);
    }
}