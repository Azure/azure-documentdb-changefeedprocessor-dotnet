//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    /// <summary>
    /// Exception occurred when partition wasn't found.
    /// </summary>
    public class PartitionNotFoundException : PartitionException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionNotFoundException"/> class.
        /// </summary>
        /// <param name="lastContinuation"> Request continuation token </param>
        public PartitionNotFoundException(string lastContinuation)
            : base(lastContinuation)
        {
        }
    }
}