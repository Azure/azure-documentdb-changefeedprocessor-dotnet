//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    /// <summary>
    /// Factory class used to create instance(s) of <see cref="IChangeFeedObserverObsolete"/>.
    /// </summary>
    public interface IChangeFeedObserverFactoryObsolete
    {
        /// <summary>
        /// Creates a new instance of <see cref="IChangeFeedObserverObsolete"/>.
        /// </summary>
        /// <returns>Created instance of <see cref="IChangeFeedObserverObsolete"/>.</returns>
        IChangeFeedObserverObsolete CreateObserver();
    }
}