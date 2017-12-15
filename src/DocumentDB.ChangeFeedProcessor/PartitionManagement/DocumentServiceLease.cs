using System;
using System.Globalization;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    internal class DocumentServiceLease : ILease
    {
        private static readonly DateTime UnixStartTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        public DocumentServiceLease()
        {
        }

        public DocumentServiceLease(DocumentServiceLease other)
        {
            Id = other.Id;
            PartitionId = other.PartitionId;
            Owner = other.Owner;
            ContinuationToken = other.ContinuationToken;
            ETag = other.ETag;
            TS = other.TS;
            ExplicitTimestamp = other.ExplicitTimestamp;
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

        [JsonProperty("timestamp")]
        private DateTime? ExplicitTimestamp { get; set; }

        [JsonIgnore]
        public DateTime Timestamp
        {
            get { return ExplicitTimestamp ?? UnixStartTime.AddSeconds(TS); }
            set { ExplicitTimestamp = value; }
        }

        [JsonIgnore]
        public string ConcurrencyToken => ETag;

        [JsonProperty("_ts")]
        private long TS { get; set; }

        public override string ToString()
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "{0} Owner='{1}' Continuation={2} Timestamp(local)={3}",
                Id,
                Owner,
                ContinuationToken,
                Timestamp.ToLocalTime());
        }

        public static DocumentServiceLease FromDocument(Document document)
        {
            if (document == null)
                throw new ArgumentNullException(nameof(document));
            string json = JsonConvert.SerializeObject(document);
            return JsonConvert.DeserializeObject<DocumentServiceLease>(json);
        }
    }
}