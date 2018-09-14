//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring
{
    /// <summary>
    /// The health severity level
    /// </summary>
    public enum HealthSeverity
    {
        /// <summary>
        /// Error level.
        /// </summary>
        Error,

        /// <summary>
        /// Information level.
        /// </summary>
        Informational,
    }
}