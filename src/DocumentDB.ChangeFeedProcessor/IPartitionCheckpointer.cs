//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System.Threading.Tasks;

#if PRIVATE_API
    public
#else
    internal
#endif
    interface IPartitionCheckpointer
    {
        /// <summary>
        /// Checkpoint the given partition up to the given continuation token.
        /// </summary>
        /// <param name="сontinuationToken">Continuation token</param>
        Task CheckpointPartitionAsync(string сontinuationToken);
    }
}