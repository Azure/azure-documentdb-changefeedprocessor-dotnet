//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    /// <summary>
    /// Factory class used to create instance(s) of <see cref="IChangeFeedObserver"/>.
    /// </summary>
    public interface IChangeFeedObserverFactory
    {
        /// <summary>
        /// Creates an instance of a <see cref="IChangeFeedObserver"/>.
        /// </summary>
        /// <returns>An instance of a <see cref="IChangeFeedObserver"/>.</returns>
        IChangeFeedObserver CreateObserver();
    }
}