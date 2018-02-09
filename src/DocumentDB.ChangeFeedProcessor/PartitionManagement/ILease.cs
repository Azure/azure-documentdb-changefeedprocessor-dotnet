//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;

    public interface ILease
    {
        string PartitionId { get; }
        string Owner { get; set; }
        DateTime Timestamp { get; set; }
        string ContinuationToken { get; set; }
        string Id { get; }
        string ConcurrencyToken { get; }
    }
}