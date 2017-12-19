//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    public interface IPartitionCheckpointer
    {
        /// <summary>
        /// Checkpoint the given partition up to the given continuation token. 
        /// </summary>
        /// <param name="сontinuationToken"></param>
        Task CheckpointPartitionAsync(string сontinuationToken);
    }
}