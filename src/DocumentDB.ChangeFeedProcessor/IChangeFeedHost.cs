//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    public interface IChangeFeedHost
    {
        Task StartAsync();
        Task StopAsync();
        Task<long> GetEstimatedRemainingWork();
    }
}