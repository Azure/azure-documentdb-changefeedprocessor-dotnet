//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.IntegrationTests
{
    class TestObserver : IChangeFeedObserver
    {
        private readonly IChangeFeedObserver parent;

        public TestObserver(IChangeFeedObserver parent)
        {
            Debug.Assert(parent != null);
            this.parent = parent;
        }

        public Task OpenAsync(ChangeFeedObserverContext context)
        {
            return parent.OpenAsync(context);
        }

        public Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return parent.CloseAsync(context, reason);
        }

        public Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs)
        {
            return this.parent.ProcessChangesAsync(context, docs);
        }
    }
}
