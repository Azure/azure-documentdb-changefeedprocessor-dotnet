//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    internal class ChangeFeedObserverFactory<T> : IChangeFeedObserverFactory
        where T : IChangeFeedObserver, new()
    {
        public IChangeFeedObserver CreateObserver()
        {
            return new T();
        }
    }
}