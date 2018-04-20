//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represents a lease that is persisted as a document in the lease collection.
    /// Leases are used to:
    /// * Keep track of the <see cref="IChangeFeedProcessor"/> progress for a particular Partition Key Range.
    /// * Distribute load between different instances of <see cref="IChangeFeedProcessor"/>.
    /// * Ensure reliable recovery for cases when an instance of <see cref="IChangeFeedProcessor"/> gets disconnected, hangs or crashes.
    /// </summary>
    public interface ILease
    {
        /// <summary>
        /// Gets the partition associated with the lease.
        /// </summary>
        string PartitionId { get; }

        /// <summary>
        /// Gets or sets the host name owner of the lease.
        /// The Owner keeps track which <see cref="IChangeFeedProcessor"/> is currently processing that Partition Key Range.
        /// </summary>
        string Owner { get; set; }

        /// <summary>
        /// Gets or sets the Timestamp of the lease.
        /// Timestamp is used to determine lease expiration.
        /// </summary>
        DateTime Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the Continuation Token.
        /// Continuation Token is used to determine the last processed point of the Change Feed.
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

        /// <summary>
        /// Gets or sets custom lease properties which can be managed from <see cref="IParitionLoadBalancingStrategy"/>.
        /// </summary>
        Dictionary<string, string> Properties { get; set; }
    }
}