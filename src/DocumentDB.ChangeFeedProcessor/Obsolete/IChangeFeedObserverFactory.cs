//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    /// <summary>
    /// Factory class used to create instance(s) of <see cref="IChangeFeedObserver"/>.
    /// </summary>
    [Obsolete]
    public interface IChangeFeedObserverFactory
    {
        /// <summary>
        /// Creates a new instance of <see cref="IChangeFeedObserver"/>.
        /// </summary>
        /// <returns>Created instance of <see cref="IChangeFeedObserver"/>.</returns>
        IChangeFeedObserver CreateObserver();
    }
}