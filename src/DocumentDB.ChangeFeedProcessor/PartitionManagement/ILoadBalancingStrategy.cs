using System.Collections.Generic;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    internal interface ILoadBalancingStrategy
    {
        IEnumerable<ILease> CalculateLeasesToTake(IEnumerable<ILease> allLeases);
    }
}