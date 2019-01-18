//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    using System;

    /// <summary>
    /// Exception occurred when ProcessChangesAsync is running and thrown by user code
    /// </summary>
    [Serializable]
    public class UserException : Exception
    {
        private static readonly string DefaultMessage = "Exception has been thrown by ProcessChangesAsync.";

        /// <summary>
        /// Initializes a new instance of the <see cref="UserException" /> class using the specified internal exception.
        /// </summary>
        /// <param name="internalException"><see cref="Exception"/> thrown by the user code.</param>
        public UserException(Exception internalException)
            : base(UserException.DefaultMessage, internalException)
        {
        }
    }
}