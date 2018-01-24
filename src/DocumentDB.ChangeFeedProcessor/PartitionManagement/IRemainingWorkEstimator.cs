//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IRemainingWorkEstimator
    {
        Task<long> GetEstimatedRemainingWork();
    }
}
