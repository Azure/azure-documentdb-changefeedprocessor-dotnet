﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Estimator
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;

    internal class RemainingWorkEstimator : IRemainingWorkEstimator
    {
        private const string LSNPropertyName = "_lsn";
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly IChangeFeedDocumentClient feedDocumentClient;
        private readonly ILeaseManager leaseManager;
        private readonly string collectionSelfLink;
        private readonly int degreeOfParallelism;

        public RemainingWorkEstimator(ILeaseManager leaseManager, IChangeFeedDocumentClient feedDocumentClient, string collectionSelfLink, int degreeOfParallelism)
        {
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));
            if (string.IsNullOrEmpty(collectionSelfLink)) throw new ArgumentNullException(nameof(collectionSelfLink));
            if (feedDocumentClient == null) throw new ArgumentNullException(nameof(feedDocumentClient));
            if (degreeOfParallelism < 1 || degreeOfParallelism > ServicePointManager.DefaultConnectionLimit) throw new ArgumentException("Degree of parallelism is out of range", nameof(degreeOfParallelism));

            this.leaseManager = leaseManager;
            this.collectionSelfLink = collectionSelfLink;
            this.feedDocumentClient = feedDocumentClient;
            this.degreeOfParallelism = degreeOfParallelism;
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            RemainingPartitionWork[] partitions = await this.GetEstimatedPartitionsRemainingWork();
            if (partitions == null) return 0;

            return partitions.Sum(partition => partition.RemainingWork);
        }

        public async Task<RemainingPartitionWork[]> GetEstimatedPartitionsRemainingWork()
        {
            IReadOnlyList<ILease> leases = await this.leaseManager.ListAllLeasesAsync().ConfigureAwait(false);
            if (leases.Count == 0)
            {
                return new RemainingPartitionWork[0];
            }

            List<Task<IList<RemainingPartitionWork>>> tasks = null;
            try
            {
                var queue = new ConcurrentQueue<ILease>(leases);
                tasks = Enumerable.Range(1, this.degreeOfParallelism).Select(async _ =>
                {
                    IList<RemainingPartitionWork> results = new List<RemainingPartitionWork>();
                    ILease item;
                    while (queue.TryDequeue(out item))
                    {
                        try
                        {
                            if (string.IsNullOrEmpty(item.PartitionId)) continue;
                            var result = await this.GetRemainingWorkForLeaseAsync(item);
                            results.Add(new RemainingPartitionWork(item.PartitionId, result));
                        }
                        catch (Exception ex)
                        {
                            Logger.WarnException($"Getting estimated work for {item.PartitionId} failed!", ex);
                        }
                    }

                    return results;
                }).ToList();

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Logger.WarnException("Incomplete estimation results!", ex);
            }

            return tasks?.SelectMany(t => t.Result).ToArray();
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
            if (!long.TryParse(number, NumberStyles.Any, CultureInfo.InvariantCulture, out parsed))
            {
                Logger.WarnFormat(string.Format(CultureInfo.InvariantCulture, "Cannot parse number '{0}'.", number));
                return 0;
            }

            return parsed;
        }

        private static string ExtractLSNFromSessionToken(string sessionToken)
        {
            if (string.IsNullOrEmpty(sessionToken))
            {
                return string.Empty;
            }

            int separatorIndex = sessionToken.IndexOf(':');
            return sessionToken.Substring(separatorIndex + 1);
        }

        private async Task<long> GetRemainingWorkForLeaseAsync(ILease existingLease)
        {
            ChangeFeedOptions options = new ChangeFeedOptions
            {
                MaxItemCount = 1,
            };
            options.PartitionKeyRangeId = existingLease.PartitionId;
            options.RequestContinuation = existingLease.ContinuationToken;
            options.StartFromBeginning = string.IsNullOrEmpty(existingLease.ContinuationToken);
            IChangeFeedDocumentQuery<Document> query = this.feedDocumentClient.CreateDocumentChangeFeedQuery(this.collectionSelfLink, options);
            IFeedResponse<Document> response = null;

            try
            {
                response = await query.ExecuteNextAsync<Document>().ConfigureAwait(false);
                long parsedLSNFromSessionToken = TryConvertToNumber(ExtractLSNFromSessionToken(response.SessionToken));
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
