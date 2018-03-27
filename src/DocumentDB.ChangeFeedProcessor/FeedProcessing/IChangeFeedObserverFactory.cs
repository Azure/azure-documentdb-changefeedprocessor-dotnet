//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    /// <summary>
    /// Factory implementation to create Observers.
    /// </summary>
    public interface IChangeFeedObserverFactory
    {
        /// <summary>
        /// Creates an instance of a <see cref="IChangeFeedObserver"/>
        /// </summary>
        /// <returns>An instance of a <see cref="IChangeFeedObserver"/>.</returns>
        IChangeFeedObserver CreateObserver();
    }
}