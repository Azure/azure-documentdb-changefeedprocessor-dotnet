//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Collections.Generic;

    internal interface ILoadBalancingStrategy
    {
        IEnumerable<ILease> CalculateLeasesToTake(IEnumerable<ILease> allLeases);
    }
}