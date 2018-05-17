//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.DataAccess;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.Client;

    internal class RemainingWorkEstimator : IRemainingWorkEstimator
    {
        private const string LSNPropertyName = "_lsn";
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly IChangeFeedDocumentClient feedDocumentClient;
        private readonly ILeaseManager leaseManager;
        private readonly string collectionSelfLink;

        public RemainingWorkEstimator(ILeaseManager leaseManager, IChangeFeedDocumentClient feedDocumentClient, string collectionSelfLink)
        {
            if (leaseManager == null) throw new ArgumentNullException(nameof(leaseManager));

            if (string.IsNullOrEmpty(collectionSelfLink)) throw new ArgumentNullException(nameof(collectionSelfLink));

            if (feedDocumentClient == null) throw new ArgumentNullException(nameof(feedDocumentClient));

            this.leaseManager = leaseManager;
            this.collectionSelfLink = collectionSelfLink;
            this.feedDocumentClient = feedDocumentClient;
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            long remainingWork = 0;
            bool hasAtLeastOneLease = false;

            ChangeFeedOptions options = new ChangeFeedOptions
            {
                MaxItemCount = 1,
            };

            foreach (ILease existingLease in await this.leaseManager.ListAllLeasesAsync().ConfigureAwait(false))
            {
                hasAtLeastOneLease = true;
                options.PartitionKeyRangeId = existingLease.PartitionId;
                options.RequestContinuation = existingLease.ContinuationToken;
                options.StartFromBeginning = string.IsNullOrEmpty(existingLease.ContinuationToken);

                IChangeFeedDocumentQuery<Document> query = this.feedDocumentClient.CreateDocumentChangeFeedQuery(this.collectionSelfLink, options);
                IFeedResponse<Document> response = null;

                try
                {
                    response = await query.ExecuteNextAsync<Document>().ConfigureAwait(false);
                    long parsedLSNFromSessionToken = TryConvertToNumber(ExtractLSNFromSessionToken(response.SessionToken));
                    long lastQueryLSN = response.Count > 0 ?
                        TryConvertToNumber(GetFirstDocument(response).GetPropertyValue<string>(LSNPropertyName)) - 1
                        : parsedLSNFromSessionToken;
                    if (lastQueryLSN < 0)
                    {
                        // Could not parse LSN from document, we cannot determine the amount of changes but since the query returned 1 document, we know it's at least 1
                        remainingWork += 1;
                        continue;
                    }

                    long partitionRemainingWork = parsedLSNFromSessionToken - lastQueryLSN;
                    remainingWork += partitionRemainingWork < 0 ? 0 : partitionRemainingWork;
                }
                catch (DocumentClientException clientException)
                {
                    Logger.WarnException("GetEstimateWork > exception: partition '{0}'", clientException, existingLease.PartitionId);
                }
            }

            if (!hasAtLeastOneLease)
            {
                return 1;
            }

            return remainingWork;
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
    }
}
