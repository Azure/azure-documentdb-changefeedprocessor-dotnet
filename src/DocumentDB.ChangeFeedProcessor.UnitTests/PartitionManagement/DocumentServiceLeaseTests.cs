//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------
namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.PartitionManagement
{
    using System.Collections.Generic;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Xunit;

    [Trait("Category", "Gated")]
    public class DocumentServiceLeaseTests
    {

        [Fact]
        public void LeaseProperties_DeserializeProperty()
        {
            Document doc = new Document();
            var properties = new Dictionary<string, string> { { "key1", "value1" }, { "key2", "value2" } };
            doc.SetPropertyValue("properties", properties);

            var lease = DocumentServiceLease.FromDocument(doc);
            Assert.Equal(properties, lease.Properties);
        }
    }
}
