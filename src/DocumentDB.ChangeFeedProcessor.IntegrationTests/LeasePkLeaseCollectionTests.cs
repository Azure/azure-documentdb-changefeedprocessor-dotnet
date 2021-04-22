//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
    using Xunit;

    /// <summary>
    /// To truly test this class, run emulator with /EnableGremlinEndpoint
    /// </summary>
    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public class LeasePkLeaseCollectionTests : StaticCollectionTests
    {
        public LeasePkLeaseCollectionTests() :
            base(true,true)
        {
        }
    }
}
