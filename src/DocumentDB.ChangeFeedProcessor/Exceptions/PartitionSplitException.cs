﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    public class PartitionSplitException : PartitionException
    {
        public PartitionSplitException(string lastContinuation)
            : base(lastContinuation)
        {
        }
    }
}