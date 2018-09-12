//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring
{
    /// <summary>
    /// The healthiness level
    /// </summary>
    public enum HealthEventLevel
    {
        /// <summary>
        /// Error level
        /// </summary>
        Error,

        /// <summary>
        /// Health level
        /// </summary>
        Health,
    }
}