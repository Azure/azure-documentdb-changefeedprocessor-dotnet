//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception occurred during partition split.
    /// </summary>
    public class PartitionSplitException : PartitionException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionSplitException" /> class.
        /// </summary>
        public PartitionSplitException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionSplitException"/> class using error message.
        /// </summary>
        /// <param name="message">The exception error message.</param>
        public PartitionSplitException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionSplitException"/> class using error message and last continuation token.
        /// </summary>
        /// <param name="message">The exception error message.</param>
        /// <param name="lastContinuation"> Request continuation token.</param>
        public PartitionSplitException(string message, string lastContinuation)
            : base(message, lastContinuation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionSplitException" /> class using error message and inner exception.
        /// </summary>
        /// <param name="message">The exception error message.</param>
        /// <param name="innerException">The inner exception.</param>
        public PartitionSplitException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionSplitException" /> class using default values.
        /// </summary>
        /// <param name="info">The SerializationInfo object that holds serialized object data for the exception being thrown.</param>
        /// <param name="context">The StreamingContext that contains contextual information about the source or destination.</param>
        protected PartitionSplitException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}