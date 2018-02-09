//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading.Tasks;

    internal interface IPartitionManager
    {
        Task StartAsync();

        Task StopAsync();
    }
}
