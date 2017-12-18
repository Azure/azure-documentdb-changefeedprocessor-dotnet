//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal interface ILoadBalancingStrategy
    {
        IEnumerable<ILease> CalculateLeasesToTake(IEnumerable<ILease> allLeases);
    }
}