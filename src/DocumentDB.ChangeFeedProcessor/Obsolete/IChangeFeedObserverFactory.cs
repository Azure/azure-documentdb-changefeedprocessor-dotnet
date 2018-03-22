//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;

    /// <summary>
    /// Factory class used to create instance(s) of <see cref="IChangeFeedObserver"/>.
    /// </summary>
    [Obsolete("Switch to ChangeFeedHostBuilder for building the change feed processor host and use new interface with cancellation token support")]
    public interface IChangeFeedObserverFactory
    {
        /// <summary>
        /// Creates a new instance of <see cref="IChangeFeedObserver"/>.
        /// </summary>
        /// <returns>Created instance of <see cref="IChangeFeedObserver"/>.</returns>
        IChangeFeedObserver CreateObserver();
    }
}