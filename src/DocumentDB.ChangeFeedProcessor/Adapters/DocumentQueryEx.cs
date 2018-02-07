//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    internal class DocumentQueryEx<T> : IDocumentQueryEx<Document>
    {
        private readonly IDocumentQuery<T> query;

        public DocumentQueryEx(IDocumentQuery<T> query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));
            this.query = query;
        }

        public bool HasMoreResults => this.query.HasMoreResults;

        public void Dispose()
        {
            this.query.Dispose();
        }

        public async Task<IFeedResponse<TResult>> ExecuteNextAsync<TResult>(CancellationToken token = new CancellationToken())
        {
            return await this.query.ExecuteNextAsync<TResult>(token).ConfigureAwait(false);
        }
    }
}