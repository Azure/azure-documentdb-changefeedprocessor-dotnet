//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
    using Xunit;

    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public class StaticPartitionedLeaseCollectionTests : StaticCollectionTests
    {
        public StaticPartitionedLeaseCollectionTests(IntegrationTestFixture fixture) :
            base(fixture, typeof(StaticPartitionedLeaseCollectionTests), true)
        {
        }
    }
}
