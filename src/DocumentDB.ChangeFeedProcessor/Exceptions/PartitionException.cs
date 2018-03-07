//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    using System;

    internal class PartitionException : Exception
    {
        public PartitionException(string lastContinuation)
        {
            this.LastContinuation = lastContinuation;
        }

        public string LastContinuation { get; }
    }
}