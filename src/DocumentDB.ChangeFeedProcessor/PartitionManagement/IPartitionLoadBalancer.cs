//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Threading.Tasks;

#if PRIVATE_API
    public
#else
    internal
#endif
    interface IPartitionLoadBalancer
    {
        /// <summary>
        /// Starts the load balancer
        /// </summary>
        void Start();

        /// <summary>
        /// Stops the load balancer
        /// </summary>
        /// <returns>Task that completes once load balancer is fully stopped</returns>
        Task StopAsync();
    }
}