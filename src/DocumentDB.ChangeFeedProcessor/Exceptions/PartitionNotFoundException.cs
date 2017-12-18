//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    public class PartitionNotFoundException : PartitionException
    {
        public PartitionNotFoundException(string lastContinuation) : base(lastContinuation)
        {
        }
    }
}