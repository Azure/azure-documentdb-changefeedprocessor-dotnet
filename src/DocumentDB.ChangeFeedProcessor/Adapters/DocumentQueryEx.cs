using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace DocumentDB.ChangeFeedProcessor.Adapters
{
    internal class DocumentQueryEx<T> : IDocumentQueryEx<Document>
    {
        private readonly IDocumentQuery<T> query;

        public DocumentQueryEx(IDocumentQuery<T> query)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));
            this.query = query;
        }

        public void Dispose()
        {
            query.Dispose();
        }

        public bool HasMoreResults => query.HasMoreResults;

        public async Task<IFeedResponse<TResult>> ExecuteNextAsync<TResult>(CancellationToken token = new CancellationToken())
        {
            return await query.ExecuteNextAsync<TResult>(token).ConfigureAwait(false);
        }
    }
}