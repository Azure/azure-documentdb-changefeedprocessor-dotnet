//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IChangeFeedProcessor: IRemainingWorkEstimator
    {
        Task StartAsync();
        Task StopAsync();
    }
}