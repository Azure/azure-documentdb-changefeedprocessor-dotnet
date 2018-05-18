//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    /// <summary>
    /// Exception occurred during partition split.
    /// </summary>
    public class PartitionSplitException : PartitionException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionSplitException"/> class.
        /// </summary>
        /// <param name="lastContinuation"> Request continuation token </param>
        public PartitionSplitException(string lastContinuation)
            : base(lastContinuation)
        {
        }
    }
}