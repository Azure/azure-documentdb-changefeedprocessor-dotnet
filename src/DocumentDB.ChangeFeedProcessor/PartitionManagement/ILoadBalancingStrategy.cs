using System.Collections.Generic;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal interface ILoadBalancingStrategy
    {
        IEnumerable<ILease> CalculateLeasesToTake(IEnumerable<ILease> allLeases);
    }
}