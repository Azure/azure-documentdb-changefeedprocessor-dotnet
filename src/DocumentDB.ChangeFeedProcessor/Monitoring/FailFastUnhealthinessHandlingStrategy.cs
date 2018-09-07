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
    /// A strategy which crashes the process. It calls Environment.FailFast
    /// </summary>
    public class FailFastUnhealthinessHandlingStrategy : IUnhealthinessHandlingStrategy
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        /// <inheritdoc />
        public Task HandleAsync(ILease lease, Exception exception)
        {
            Logger.ErrorException($"Unhealthy instance detected. Last aquired lease {lease}", exception);
            Environment.FailFast("Unhealthy instance detected.");

            return Task.FromResult(true);
        }
    }
}