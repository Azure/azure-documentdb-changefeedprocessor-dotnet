//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    using System;

    public class PartitionException : Exception
    {
        public string LastContinuation { get; }

        public PartitionException(string lastContinuation)
        {
            LastContinuation = lastContinuation;
        }
    }
}