//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Adapters;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public class RemainingWorkEstimator : IRemainingWorkEstimator
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();
        private readonly IDocumentClientEx feedDocumentClient;
        private readonly ILeaseManager leaseManager;
        private readonly string collectionSelfLink;
        private const string LSNPropertyName = "_lsn";

        public RemainingWorkEstimator(ILeaseManager leaseManager, IDocumentClientEx feedDocumentClient, string collectionSelfLink)
        {
            this.leaseManager = leaseManager;
            this.collectionSelfLink = collectionSelfLink;
            this.feedDocumentClient = feedDocumentClient;
        }

        public async Task<long> GetEstimatedRemainingWork()
        {
            long remainingWork = 0;

            ChangeFeedOptions options = new ChangeFeedOptions
            {
                MaxItemCount = 1
            };

            foreach (ILease existingLease in await this.leaseManager.ListLeasesAsync())
            {
                options.PartitionKeyRangeId = existingLease.PartitionId;
                options.RequestContinuation = existingLease.ContinuationToken;

                IDocumentQueryEx<Document> query = this.feedDocumentClient.CreateDocumentChangeFeedQuery(this.collectionSelfLink, options);
                IFeedResponse<Document> response = null;

                try
                {
                    response = await query.ExecuteNextAsync<Document>();
                    long parsedLSNFromSessionToken = TryConvertToNumber(ExtractLSNFromSessionToken(response.SessionToken));
                    long lastSequenceNumber = response.Count > 0 ? 
                        TryConvertToNumber(GetFirstDocument(response).GetPropertyValue<string>(LSNPropertyName)) 
                        : parsedLSNFromSessionToken;
                    long partitionRemainingWork = parsedLSNFromSessionToken - lastSequenceNumber;
                    remainingWork += partitionRemainingWork < 0 ? 0 : partitionRemainingWork;
                }
                catch (DocumentClientException clientException)
                {
                    logger.WarnException("GetEstimateWork > exception: partition '{0}'", clientException, existingLease.PartitionId);
                }
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
                logger.WarnFormat(string.Format(CultureInfo.InvariantCulture, "Cannot parse number '{0}'.", number));
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
