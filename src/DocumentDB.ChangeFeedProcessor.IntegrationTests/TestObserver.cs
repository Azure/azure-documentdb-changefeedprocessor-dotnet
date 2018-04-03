//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
#pragma warning disable CS0618 // Type or member is obsolete

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
