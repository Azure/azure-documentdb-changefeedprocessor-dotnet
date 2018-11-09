//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.Client;

    internal class RemainingWorkEstimator : IRemainingWorkEstimator
    {
        private const char PKRangeIdSeparator = ':';
        private const char SegmentSeparator = '#';
        private const string LSNPropertyName = "_lsn";
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly IChangeFeedDocumentClient feedDocumentClient;
        private readonly ILeaseContainer leaseContainer;
        private readonly string collectionSelfLink;
        private readonly int degreeOfParallelism;

        public RemainingWorkEstimator(
            ILeaseContainer leaseContainer,
            IChangeFeedDocumentClient feedDocumentClient,
            string collectionSelfLink,
            int degreeOfParallelism)
        {
            if (leaseContainer == null) throw new ArgumentNullException(nameof(leaseContainer));
            if (string.IsNullOrEmpty(collectionSelfLink)) throw new ArgumentNullException(nameof(collectionSelfLink));
            if (feedDocumentClient == null) throw new ArgumentNullException(nameof(feedDocumentClient));
            if (degreeOfParallelism < 1) throw new ArgumentException("Degree of parallelism is out of range", nameof(degreeOfParallelism));

            this.leaseContainer = leaseContainer;
            this.collectionSelfLink = collectionSelfLink;
            this.feedDocumentClient = feedDocumentClient;
            this.degreeOfParallelism = degreeOfParallelism;
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            var partitions = await this.GetEstimatedRemainingWorkPerPartitionAsync();
            if (partitions.Count == 0) return 1;

            return partitions.Sum(partition => partition.RemainingWork);
        }

        public async Task<IReadOnlyList<RemainingPartitionWork>> GetEstimatedRemainingWorkPerPartitionAsync()
        {
            IReadOnlyList<ILease> leases = await this.leaseContainer.GetAllLeasesAsync().ConfigureAwait(false);
            if (leases == null || leases.Count == 0)
            {
                return new List<RemainingPartitionWork>().AsReadOnly();
            }

            var tasks = Partitioner.Create(leases)
                .GetPartitions(this.degreeOfParallelism)
                .Select(partition => Task.Run(async () =>
                {
                    var partialResults = new List<RemainingPartitionWork>();
                    using (partition)
                    {
                        while (partition.MoveNext())
                        {
                            ILease item = partition.Current;
                            try
                            {
                                if (string.IsNullOrEmpty(item?.PartitionId)) continue;
                                var result = await this.GetRemainingWorkAsync(item);
                                partialResults.Add(new RemainingPartitionWork(item.PartitionId, result));
                            }
                            catch (DocumentClientException ex)
                            {
                                Logger.WarnException($"Getting estimated work for {item.PartitionId} failed!", ex);
                            }
                        }
                    }

                    return partialResults;
                })).ToArray();

            var results = await Task.WhenAll(tasks);
            return results.SelectMany(r => r).ToList().AsReadOnly();
        }

        /// <summary>
        /// Parses a Session Token and extracts the LSN.
        /// </summary>
        /// <remarks>
        /// Session Token can be in two formats. Either {PartitionKeyRangeId}:{LSN} or {PartitionKeyRangeId}:{Version}#{GlobalLSN}.
        /// </remarks>
        /// <param name="sessionToken">A Session Token</param>
        /// <returns>Lsn value</returns>
        internal static string ExtractLsnFromSessionToken(string sessionToken)
        {
            if (string.IsNullOrEmpty(sessionToken))
            {
                return string.Empty;
            }

            string parsedSessionToken = sessionToken.Substring(sessionToken.IndexOf(RemainingWorkEstimator.PKRangeIdSeparator) + 1);
            string[] segments = parsedSessionToken.Split(RemainingWorkEstimator.SegmentSeparator);

            if (segments.Length < 2)
            {
                return segments[0];
            }

            // GlobalLsn
            return segments[1];
        }

        private static Document GetFirstDocument(IFeedResponse<Document> response)
        {
            using (IEnumerator<Document> e = response.GetEnumerator())
            {
                while (e.MoveNext())
                {
                    return e.Current;
                }
            }

            return null;
        }

        private static long TryConvertToNumber(string number)
        {
            long parsed = 0;
            if (!long.TryParse(number, NumberStyles.Number, CultureInfo.InvariantCulture, out parsed))
            {
                Logger.WarnFormat(string.Format(CultureInfo.InvariantCulture, "Cannot parse number '{0}'.", number));
                return 0;
            }

            return parsed;
        }

        private async Task<long> GetRemainingWorkAsync(ILease existingLease)
        {
            ChangeFeedOptions options = new ChangeFeedOptions
            {
                MaxItemCount = 1,
                PartitionKeyRangeId = existingLease.PartitionId,
                RequestContinuation = existingLease.ContinuationToken,
                StartFromBeginning = string.IsNullOrEmpty(existingLease.ContinuationToken),
            };
            IChangeFeedDocumentQuery<Document> query = this.feedDocumentClient.CreateDocumentChangeFeedQuery(this.collectionSelfLink, options);
            IFeedResponse<Document> response = null;

            try
            {
                response = await query.ExecuteNextAsync<Document>().ConfigureAwait(false);
                long parsedLSNFromSessionToken = TryConvertToNumber(ExtractLsnFromSessionToken(response.SessionToken));
                long lastQueryLSN = response.Count > 0
                    ? TryConvertToNumber(GetFirstDocument(response).GetPropertyValue<string>(LSNPropertyName)) - 1
                    : parsedLSNFromSessionToken;
                if (lastQueryLSN < 0)
                {
                    return 1;
                }

                long partitionRemainingWork = parsedLSNFromSessionToken - lastQueryLSN;
                return partitionRemainingWork < 0 ? 0 : partitionRemainingWork;
            }
            catch (Exception clientException)
            {
                Logger.WarnException($"GetEstimateWork > exception: partition '{existingLease.PartitionId}'", clientException);
                throw;
            }
        }
    }
}
