//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Collections.Generic;

    public interface ILoadBalancingStrategy
    {
        IEnumerable<ILease> SelectLeasesToTake(IEnumerable<ILease> allLeases);
    }
}