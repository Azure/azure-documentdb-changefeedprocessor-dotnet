//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;

    /// <summary>
    /// A lease is used to keep track of the Change Feed Processor's progress for a particular Partition Key Range.
    /// The Owner keeps track which <see cref="IChangeFeedProcessor"/> is currently processing that Partition Key Range and the Continuation Token is used to know what was the last processed point of the Change Feed.
    /// </summary>
    public interface ILease
    {
        /// <summary>
        /// Gets the partition associated with the lease.
        /// </summary>
        string PartitionId { get; }

        /// <summary>
        /// Gets or sets the host name owner of the lease.
        /// </summary>
        string Owner { get; set; }

        /// <summary>
        /// Gets or sets the Timestamp of the lease.
        /// </summary>
        DateTime Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the Continuation Token.
        /// </summary>
        string ContinuationToken { get; set; }

        /// <summary>
        /// Gets the lease Id.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Gets the Concurrency Token.
        /// </summary>
        string ConcurrencyToken { get; }
    }
}