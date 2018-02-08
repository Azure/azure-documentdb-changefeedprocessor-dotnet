//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils
{
    internal class CollectionHelper
    {
        internal static async Task<List<PartitionKeyRange>> EnumPartitionKeyRangesAsync(DocumentClient client, string collectionSelfLink)
        {
            Debug.Assert(client != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(collectionSelfLink), "collectionSelfLink");

            string partitionkeyRangesPath = string.Format(CultureInfo.InvariantCulture, "{0}/pkranges", collectionSelfLink);

            FeedResponse<PartitionKeyRange> response = null;
            var partitionKeyRanges = new List<PartitionKeyRange>();
            do
            {
                FeedOptions feedOptions = new FeedOptions { MaxItemCount = 1000, RequestContinuation = response != null ? response.ResponseContinuation : null };
                response = await client.ReadPartitionKeyRangeFeedAsync(partitionkeyRangesPath, feedOptions);
                partitionKeyRanges.AddRange(response);
            }
            while (!string.IsNullOrEmpty(response.ResponseContinuation));

            return partitionKeyRanges;
        }
    }
}
