// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess
{
    using System;

    /// <summary>
    /// Specifies the settings that identify unhealthy operations of <see cref="IChangeFeedDocumentClient"/>.
    /// </summary>
    public class ChangeFeedClientHealthOptions
    {
        /// <summary>Initializes a new instance of the <see cref="ChangeFeedClientHealthOptions" /> class.</summary>
        public ChangeFeedClientHealthOptions()
        {
            this.MaxChangeFeedQueryDuration = TimeSpan.FromMinutes(10);
        }

        /// <summary>
        /// Gets or sets a value indicating the maximum allowed duration of <see cref="IChangeFeedDocumentQuery{T}.ExecuteNextAsync{TResult}"/> operation.
        /// </summary>
        public TimeSpan MaxChangeFeedQueryDuration { get; set; }
    }
}
