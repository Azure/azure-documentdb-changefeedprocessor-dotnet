//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
    using Xunit;

    /// <summary>
    /// Test ChangeFeed with Custom Partition key lease collections
    /// </summary>
    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public class CustomPartitionedLeaseCollectionTests:IntegrationTest
    {
        public CustomPartitionedLeaseCollectionTests():base(isPartitionedMonitoredCollection:true, isPartitionedLeaseCollection:true,leaseCollectionPartitionKey:"leaseId")
        {
        }
    }
}
