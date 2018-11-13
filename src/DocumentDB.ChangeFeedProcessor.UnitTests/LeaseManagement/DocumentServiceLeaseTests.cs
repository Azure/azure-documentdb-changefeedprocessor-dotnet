//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------
namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.LeaseManagement
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Xunit;

    [Trait("Category", "Gated")]
    public class DocumentServiceLeaseTests
    {
        [Fact]
        public void LeaseProperties_FromDocument()
        {
            Document doc = new Document();
            var properties = new Dictionary<string, string> { { "key1", "value1" }, { "key2", "value2" } };
            doc.SetPropertyValue("properties", properties);

            var lease = DocumentServiceLease.FromDocument(doc);
            Assert.Equal(properties, lease.Properties);
        }

        [Fact]
        public void ValidateProperties()
        {
            var id = "id";
            var etag = "etag";
            var partitionId = "0";
            var owner = "owner";
            var continuationToken = "continuation";
            var timestamp = DateTime.Now - TimeSpan.FromSeconds(5);
            var key = "key";
            var value = "value";

            DocumentServiceLease lease = new DocumentServiceLease
            {
                Id = id,
                ETag = etag,
                PartitionId = partitionId,
                Owner = owner,
                ContinuationToken = continuationToken,
                Timestamp = timestamp,
                Properties = new Dictionary<string, string> { { "key", "value" } },
            };

            Assert.Equal(id, lease.Id);
            Assert.Equal(etag, lease.ETag);
            Assert.Equal(partitionId, lease.PartitionId);
            Assert.Equal(owner, lease.Owner);
            Assert.Equal(continuationToken, lease.ContinuationToken);
            Assert.Equal(timestamp, lease.Timestamp);
            Assert.Equal(value, lease.Properties[key]);
            Assert.Equal(etag, lease.ConcurrencyToken);
        }

        [Fact]
        public void ValidateSerialization_AllFields()
        {
            DocumentServiceLease originalLease = new DocumentServiceLease
            {
                Id = "id",
                ETag = "etag",
                PartitionId = "0",
                Owner = "owner",
                ContinuationToken = "continuation",
                Timestamp = DateTime.Now - TimeSpan.FromSeconds(5),
                Properties = new Dictionary<string, string> { { "key", "value" }}
            };

            var buffer = new byte[4096];
            var formatter = new BinaryFormatter();
            var stream1 = new MemoryStream(buffer);
            var stream2 = new MemoryStream(buffer);

            formatter.Serialize(stream1, originalLease);
            var lease = (DocumentServiceLease)formatter.Deserialize(stream2);

            Assert.Equal(originalLease.Id, lease.Id);
            Assert.Equal(originalLease.ETag, lease.ETag);
            Assert.Equal(originalLease.PartitionId, lease.PartitionId);
            Assert.Equal(originalLease.Owner, lease.Owner);
            Assert.Equal(originalLease.ContinuationToken, lease.ContinuationToken);
            Assert.Equal(originalLease.Timestamp, lease.Timestamp);
            Assert.Equal(originalLease.Properties["key"], lease.Properties["key"]);
        }

        // Make sure that when some fields are not set, serialization is not broken.
        [Fact]
        public void ValidateSerialization_NullFields()
        {
            DocumentServiceLease originalLease = new DocumentServiceLease();
            var buffer = new byte[4096];
            var formatter = new BinaryFormatter();
            var stream1 = new MemoryStream(buffer);
            var stream2 = new MemoryStream(buffer);

            formatter.Serialize(stream1, originalLease);
            var lease = (DocumentServiceLease)formatter.Deserialize(stream2);

            Assert.Null(lease.Id);
            Assert.Null(lease.ETag);
            Assert.Null(lease.PartitionId);
            Assert.Null(lease.Owner);
            Assert.Null(lease.ContinuationToken);
            Assert.Equal(new DocumentServiceLease().Timestamp, lease.Timestamp);
            Assert.Empty(lease.Properties);
        }
    }
}
