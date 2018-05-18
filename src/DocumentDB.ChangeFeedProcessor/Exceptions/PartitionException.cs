//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    using System;

    /// <summary>
    /// General exception occurred during partition processing.
    /// </summary>
    public class PartitionException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionException"/> class.
        /// </summary>
        /// <param name="lastContinuation"> Request continuation token </param>
        public PartitionException(string lastContinuation)
        {
            this.LastContinuation = lastContinuation;
        }

        /// <summary>
        /// Gets value of request continuation token.
        /// </summary>
        public string LastContinuation { get; }
    }
}