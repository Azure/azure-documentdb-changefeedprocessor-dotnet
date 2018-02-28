//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    internal class PartitionNotFoundException : PartitionException
    {
        public PartitionNotFoundException(string lastContinuation)
            : base(lastContinuation)
        {
        }
    }
}