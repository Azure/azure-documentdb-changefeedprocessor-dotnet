//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    internal class ObserverFactory<T> : IObserverFactory
        where T : IObserver, new()
    {
        public IObserver CreateObserver()
        {
            return new T();
        }
    }
}