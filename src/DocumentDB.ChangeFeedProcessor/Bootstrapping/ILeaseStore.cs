//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping
{
    using System;
    using System.Threading.Tasks;

    internal interface ILeaseStore
    {
        /// <summary>
        /// Checks if the lease store is initialized
        /// </summary>
        Task<bool> IsInitializedAsync();

        /// <summary>
        /// Lock the store for bootstrapping. Only one process may lock the store for the lock time
        /// </summary>
        /// <param name="lockTime">Lock time</param>
        Task<bool> LockInitializationAsync(TimeSpan lockTime);

        /// <summary>
        /// Mark the store as initialized
        /// </summary>
        Task MarkInitializedAsync();
    }
}