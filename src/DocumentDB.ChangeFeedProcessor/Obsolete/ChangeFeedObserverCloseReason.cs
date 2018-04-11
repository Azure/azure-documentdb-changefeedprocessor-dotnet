﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;

    /// <summary>
    /// The reason for the <see cref="IChangeFeedObserver"/> to close.
    /// </summary>
    [Obsolete("Switch to the ChangeFeedProcessorBuilder for building the change feed processor and use new enum Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.ChangeFeedObserverCloseReason.")]
    public enum ChangeFeedObserverCloseReason
    {
        /// <summary>
        /// Unknown failure. This should never be sent to observers.
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// The ChangeFeedEventHost is shutting down.
        /// </summary>
        Shutdown,

        /// <summary>
        /// The resource, such as database or collection was removed.
        /// </summary>
        ResourceGone,

        /// <summary>
        /// Lease was lost due to expiration or load-balancing.
        /// </summary>
        LeaseLost,

        /// <summary>
        /// IChangeFeedObserver threw an exception.
        /// </summary>
        ObserverError,

        /// <summary>
        /// The lease is gone. This can be due to partition split.
        /// </summary>
        LeaseGone,
    }
}