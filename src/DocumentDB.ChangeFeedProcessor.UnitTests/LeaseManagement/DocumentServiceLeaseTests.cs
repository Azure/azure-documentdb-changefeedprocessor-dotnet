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
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Newtonsoft.Json;
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
            Assert.Null(lease.LeasePartitionKey);
        }

        [Fact]
        public void ValidateSerialization_AllFields()
        {
            DocumentServiceLease originalLease = new DocumentServiceLease
            {
                Id = "id",
                LeasePartitionKey="leasePk",
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
            Assert.Equal(originalLease.LeasePartitionKey, lease.LeasePartitionKey);
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
            Assert.Null(lease.LeasePartitionKey);
            Assert.Null(lease.ETag);
            Assert.Null(lease.PartitionId);
            Assert.Null(lease.Owner);
            Assert.Null(lease.ContinuationToken);
            Assert.Equal(new DocumentServiceLease().Timestamp, lease.Timestamp);
            Assert.Empty(lease.Properties);
        }


        #region Compat_Tests
        // this class doesnt contain LeaseId property

        [Serializable]
        class DocumentServiceLeaseV1
        {
            private static readonly DateTime UnixStartTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            [JsonProperty("id")]
            public string Id { get; set; }

            [JsonProperty("_etag")]
            public string ETag { get; set; }

            [JsonProperty("PartitionId")]
            public string PartitionId { get; set; }

            [JsonProperty("Owner")]
            public string Owner { get; set; }

            /// <summary>
            /// Gets or sets the current value for the offset in the stream.
            /// </summary>
            [JsonProperty("ContinuationToken")]
            public string ContinuationToken { get; set; }

            [JsonIgnore]
            public DateTime Timestamp
            {
                get { return this.ExplicitTimestamp ?? UnixStartTime.AddSeconds(this.TS); }
                set { this.ExplicitTimestamp = value; }
            }

            [JsonIgnore]
            public string ConcurrencyToken => this.ETag;

            [JsonProperty("properties")]
            public Dictionary<string, string> Properties { get; set; } = new Dictionary<string, string>();

            [JsonIgnore]
            public LeaseAcquireReason AcquireReason { get; set; }

            [JsonProperty("timestamp")]
            private DateTime? ExplicitTimestamp { get; set; }

            [JsonProperty("_ts")]
            private long TS { get; set; }

        }

        [Fact]
        public void ValidateBackwardCompat_OldLeaseFormat()
        {

            DocumentServiceLeaseV1 originalLease = new DocumentServiceLeaseV1
            {
                Id = "id",
                ETag = "etag",
                PartitionId = "0",
                Owner = "owner",
                ContinuationToken = "continuation",
                Timestamp = DateTime.Now - TimeSpan.FromSeconds(5),
                Properties = new Dictionary<string, string> { { "key", "value" } }
            };

            var serializedV1Lease = JsonConvert.SerializeObject(originalLease);
            var lease = JsonConvert.DeserializeObject<DocumentServiceLease>(serializedV1Lease);

            Assert.Equal(originalLease.Id, lease.Id);
            Assert.Equal(null, lease.LeasePartitionKey);
            Assert.Equal(originalLease.ETag, lease.ETag);
            Assert.Equal(originalLease.PartitionId, lease.PartitionId);
            Assert.Equal(originalLease.Owner, lease.Owner);
            Assert.Equal(originalLease.ContinuationToken, lease.ContinuationToken);
            Assert.Equal(originalLease.Timestamp, lease.Timestamp);
            Assert.Equal(originalLease.Properties["key"], lease.Properties["key"]);
        }

        [Fact]
        public void ValidateForwardCompat_OldLeaseFormat()
        {

            DocumentServiceLease originalLease = new DocumentServiceLease
            {
                Id = "id",
                ETag = "etag",
                PartitionId = "0",
                Owner = "owner",
                ContinuationToken = "continuation",
                Timestamp = DateTime.Now - TimeSpan.FromSeconds(5),
                Properties = new Dictionary<string, string> { { "key", "value" } }
            };

            var serializedLease = JsonConvert.SerializeObject(originalLease);
            var lease = JsonConvert.DeserializeObject<DocumentServiceLeaseV1>(serializedLease);

            Assert.Equal(originalLease.Id, lease.Id);
            Assert.Equal(originalLease.ETag, lease.ETag);
            Assert.Equal(originalLease.PartitionId, lease.PartitionId);
            Assert.Equal(originalLease.Owner, lease.Owner);
            Assert.Equal(originalLease.ContinuationToken, lease.ContinuationToken);
            Assert.Equal(originalLease.Timestamp, lease.Timestamp);
            Assert.Equal(originalLease.Properties["key"], lease.Properties["key"]);
        }
        #endregion

    }
}
