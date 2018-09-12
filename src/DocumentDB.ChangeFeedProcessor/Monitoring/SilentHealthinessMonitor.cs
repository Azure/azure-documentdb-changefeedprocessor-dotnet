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
    public class SilentHealthinessMonitor : IHealthinessMonitor
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        /// <inheritdoc />
        public Task InspectAsync(HealthEventLevel level, HealthEventPhase phase, ILease lease, Exception exception = null)
        {
            if (level == HealthEventLevel.Error)
            {
                Logger.ErrorException($"Unhealthiness detected in the phase {phase} for {lease}. ", exception);
            }

            return Task.FromResult(true);
        }
    }
}