//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Obsolete.Adapters
{
    using System;

    internal class ChangeFeedObserverFactoryAdapter : FeedProcessing.IChangeFeedObserverFactory
    {
#pragma warning disable CS0618 // Type or member is obsolete
        private IChangeFeedObserverFactory factory;
#pragma warning restore CS0618 // Type or member is obsolete

#pragma warning disable CS0618 // Type or member is obsolete
        internal ChangeFeedObserverFactoryAdapter(IChangeFeedObserverFactory factory)
#pragma warning restore CS0618 // Type or member is obsolete
        {
            this.factory = factory ?? throw new ArgumentNullException(nameof(factory));
        }

        public FeedProcessing.IChangeFeedObserver CreateObserver()
        {
#pragma warning disable CS0618 // Type or member is obsolete
            IChangeFeedObserver observer = this.factory.CreateObserver();
#pragma warning restore CS0618 // Type or member is obsolete
            return new ChangeFeedObserverAdapter(observer);
        }
    }
}