//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Newtonsoft.Json;

    [Serializable]
    internal class DocumentServiceLease : ILease
    {
        private static readonly DateTime UnixStartTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        public DocumentServiceLease()
        {
        }

        public DocumentServiceLease(DocumentServiceLease other)
        {
            this.Id = other.Id;
            this.PartitionId = other.PartitionId;
            this.Owner = other.Owner;
            this.ContinuationToken = other.ContinuationToken;
            this.ETag = other.ETag;
            this.TS = other.TS;
            this.ExplicitTimestamp = other.ExplicitTimestamp;
            this.Properties = other.Properties;
        }

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

        [JsonProperty("timestamp")]
        private DateTime? ExplicitTimestamp { get; set; }

        [JsonProperty("_ts")]
        private long TS { get; set; }

        public static DocumentServiceLease FromDocument(Document document)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            string json = JsonConvert.SerializeObject(document);
            return JsonConvert.DeserializeObject<DocumentServiceLease>(json);
        }

        public override string ToString()
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "{0} Owner='{1}' Continuation={2} Timestamp(local)={3} Timestamp(server)={4}",
                this.Id,
                this.Owner,
                this.ContinuationToken,
                this.Timestamp.ToUniversalTime(),
                UnixStartTime.AddSeconds(this.TS).ToUniversalTime());
        }
    }
}