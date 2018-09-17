//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring
{
    using System.Threading.Tasks;

    /// <summary>
    /// A strategy for handling the situation when the change feed processor is not able to acquire lease due to unknown reasons.
    /// </summary>
    public interface IHealthMonitor
    {
        /// <summary>
        /// A logic to handle that exceptional situation.
        /// </summary>
        Task InspectAsync(HealthMonitoringRecord record);
    }
}