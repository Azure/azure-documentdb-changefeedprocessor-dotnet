//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
#pragma warning disable CS0618 // Type or member is obsolete

    class TestObserverFactory : IChangeFeedObserverFactory, IChangeFeedObserver
    {
        private readonly Func<ChangeFeedObserverContext, Task> openProcessor;
        private readonly Func<ChangeFeedObserverContext, ChangeFeedObserverCloseReason, Task> closeProcessor;
        private readonly Func<ChangeFeedObserverContext, IReadOnlyList<Document>, Task> changeProcessor;

        public TestObserverFactory(Func<ChangeFeedObserverContext, IReadOnlyList<Document>, Task> changeProcessor)
        {
            this.changeProcessor = changeProcessor;
        }

        public TestObserverFactory(
            Func<ChangeFeedObserverContext, Task> openProcessor,
            Func<ChangeFeedObserverContext, ChangeFeedObserverCloseReason, Task> closeProcessor,
            Func<ChangeFeedObserverContext, IReadOnlyList<Document>, Task> changeProcessor)
        {
            this.openProcessor = openProcessor;
            this.closeProcessor = closeProcessor;
            this.changeProcessor = changeProcessor;
        }

        public IChangeFeedObserver CreateObserver()
        {
            return new TestObserver(this);
        }

        public Task OpenAsync(ChangeFeedObserverContext context)
        {
            if (this.openProcessor != null) return this.openProcessor(context);
            else return Task.CompletedTask;
        }

        public Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            if (this.closeProcessor != null) return this.closeProcessor(context, reason);
            else return Task.CompletedTask;
        }

        public Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs)
        {
            if (this.changeProcessor != null) return this.changeProcessor(context, docs);
            else return Task.CompletedTask;
        }
    }
}
