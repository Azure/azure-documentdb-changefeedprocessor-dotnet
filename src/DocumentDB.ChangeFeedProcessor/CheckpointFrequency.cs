﻿// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;

    /// <summary>
    /// Specifies the frequency of lease event. The event will trigger when either of conditions is satisfied.
    /// </summary>
    public class CheckpointFrequency
    {
        /// <summary>
        /// Gets or sets a value indicating whether explicit checkpointing is enabled. By default false. Setting to true means changefeed host will never checkpoint. Client code needs to explicitly checkpoint via <see cref="IPartitionCheckpointer"/>
        /// </summary>
        public bool ExplicitCheckpoint { get; set; }

        /// <summary>
        /// Gets or sets the value that specifies to checkpoint every specified number of docs.
        /// </summary>
        public int? ProcessedDocumentCount { get; set; }

        /// <summary>
        /// Gets or sets the value that specifies to checkpoint every specified time interval.
        /// </summary>
        public TimeSpan? TimeInterval { get; set; }
    }
}