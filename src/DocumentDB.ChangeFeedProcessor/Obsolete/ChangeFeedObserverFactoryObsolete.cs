//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    internal class ChangeFeedObserverFactoryObsolete<T> : IChangeFeedObserverFactoryObsolete
        where T : IChangeFeedObserverObsolete, new()
    {
        public IChangeFeedObserverObsolete CreateObserver()
        {
            return new T();
        }
    }
}