//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Text;

    /// <summary>
    /// Exception occurred when all retries on StatusCode.NotFound/SubStatusCode.ReadSessionNotAvaialable are over.
    /// </summary>
    [Serializable]
    public class ReadSessionNotAvailableException : PartitionException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ReadSessionNotAvailableException"/> class using error message and last continuation token.
        /// </summary>
        /// <param name="message">The exception error message.</param>
        /// <param name="lastContinuation"> Request continuation token.</param>
        public ReadSessionNotAvailableException(string message, string lastContinuation)
            : base(message, lastContinuation)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadSessionNotAvailableException" /> class using error message and inner exception.
        /// </summary>
        /// <param name="message">The exception error message.</param>
        /// <param name="lastContinuation">The last known continuation token</param>
        /// <param name="innerException">The inner exception.</param>
        public ReadSessionNotAvailableException(string message, string lastContinuation, Exception innerException)
            : base(message, lastContinuation, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadSessionNotAvailableException" /> class using default values.
        /// </summary>
        /// <param name="info">The SerializationInfo object that holds serialized object data for the exception being thrown.</param>
        /// <param name="context">The StreamingContext that contains contextual information about the source or destination.</param>
        protected ReadSessionNotAvailableException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
