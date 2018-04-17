// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//  ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

    /// <summary>
    /// Factory class used to create instance(s) of <see cref="IPartitionProcessor"/>.
    /// </summary>
    public interface IPartitionProcessorFactory
    {
        /// <summary>
        /// Creates an instance of a <see cref="IPartitionProcessor"/>.
        /// </summary>
        /// <param name="observer">Observer to be used</param>
        /// <param name="lease">Lease to be used for partition processing</param>
        /// <returns>An instance of a <see cref="IPartitionProcessor"/>.</returns>
        IPartitionProcessor Create(IChangeFeedObserver observer, ILease lease);
    }
}
