//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    /// <summary>
    /// Provides methods to support change feed query pagination and asynchronous execution
    /// </summary>
    /// <typeparam name="T">Generic type of Document</typeparam>
    public interface IChangeFeedDocumentQuery<T> : IDocumentQuery
    {
        /// <summary>
        /// Gets a value indicating whether there are potentially additional results that can be
        /// returned from the query in the Azure DocumentDB database service.
        /// </summary>
        /// <value>Boolean value representing if there are potentially additional results that can be
        /// returned from the query.</value>
        /// <remarks>Initially returns true. This value is set based on whether the last execution returned a continuation token.</remarks>
        bool HasMoreResults { get; }

        /// <summary>
        /// Executes the query and retrieves the next page of results in the Azure DocumentDB database service.
        /// </summary>
        /// <typeparam name="TResult">The type of the object returned in the query result.</typeparam>
        /// <param name="token">(Optional) The <see cref="T:System.Threading.CancellationToken" /> allows for notification that operations should be cancelled.</param>
        /// <returns>The Task object for the asynchronous response from query execution.</returns>
        Task<IFeedResponse<TResult>> ExecuteNextAsync<TResult>(CancellationToken token = default(CancellationToken));
    }
}