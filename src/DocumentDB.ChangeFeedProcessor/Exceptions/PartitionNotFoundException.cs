//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception occurred when partition wasn't found.
    /// </summary>
    public class PartitionNotFoundException : PartitionException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionNotFoundException" /> class.
        /// </summary>
        public PartitionNotFoundException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionNotFoundException"/> class.
        /// </summary>
        /// <param name="lastContinuation"> Request continuation token </param>
        public PartitionNotFoundException(string lastContinuation)
            : base(lastContinuation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionNotFoundException" /> class using error message and inner exception.
        /// </summary>
        /// <param name="message">The exception error message.</param>
        /// <param name="innerException">The inner exception.</param>
        public PartitionNotFoundException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionNotFoundException" /> class using default values.
        /// </summary>
        /// <param name="info">The SerializationInfo object that holds serialized object data for the exception being thrown.</param>
        /// <param name="context">The StreamingContext that contains contextual information about the source or destination.</param>
        protected PartitionNotFoundException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}