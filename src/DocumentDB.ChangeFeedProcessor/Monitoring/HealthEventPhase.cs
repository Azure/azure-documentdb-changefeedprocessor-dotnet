﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Monitoring
{
    /// <summary>
    /// The health monitoring phase
    /// </summary>
    public enum HealthEventPhase
    {
        /// <summary>
        /// A phase when the instance tries to aquire the lease
        /// </summary>
        AquireLease,
    }
}