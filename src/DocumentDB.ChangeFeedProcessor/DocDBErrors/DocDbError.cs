//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DocDBErrors
{
    internal enum DocDbError
    {
        Undefined,
        PartitionNotFound,
        PartitionSplit,
        TransientError,
        MaxItemCountTooLarge,
    }
}