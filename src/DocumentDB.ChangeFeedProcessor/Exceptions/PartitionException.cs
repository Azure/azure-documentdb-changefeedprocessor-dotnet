//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// General exception occurred during partition processing.
    /// </summary>
    public class PartitionException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionException" /> class.
        /// </summary>
        public PartitionException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionException"/> class.
        /// </summary>
        /// <param name="lastContinuation"> Request continuation token </param>
        public PartitionException(string lastContinuation)
        {
            this.LastContinuation = lastContinuation;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionException" /> class using error message and inner exception.
        /// </summary>
        /// <param name="message">The exception error message.</param>
        /// <param name="innerException">The inner exception.</param>
        public PartitionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionException" /> class using default values.
        /// </summary>
        /// <param name="info">The SerializationInfo object that holds serialized object data for the exception being thrown.</param>
        /// <param name="context">The StreamingContext that contains contextual information about the source or destination.</param>
        protected PartitionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        /// <summary>
        /// Gets value of request continuation token.
        /// </summary>
        public string LastContinuation { get; }
    }
}