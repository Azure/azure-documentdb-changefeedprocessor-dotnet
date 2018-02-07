//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading.Tasks;

    public interface IChangeFeedProcessor
    {
        Task StartAsync();

        Task StopAsync();
    }
}