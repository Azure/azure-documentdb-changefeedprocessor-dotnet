﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DocDBErrors
{
    using System.Net;
    using Microsoft.Azure.Documents;

    internal static class ExceptionClassifier
    {
        public static DocDbError ClassifyClientException(DocumentClientException clientException)
        {
            SubStatusCode subStatusCode = clientException.GetSubStatusCode();

            if (clientException.StatusCode == HttpStatusCode.NotFound && subStatusCode != SubStatusCode.ReadSessionNotAvailable)
                return DocDbError.PartitionNotFound;

            if (clientException.StatusCode == HttpStatusCode.Gone && (subStatusCode == SubStatusCode.PartitionKeyRangeGone || subStatusCode == SubStatusCode.Splitting))
                return DocDbError.PartitionSplit;

            if (clientException.StatusCode == (HttpStatusCode)429 || clientException.StatusCode >= HttpStatusCode.InternalServerError)
                return DocDbError.TransientError;

            // Temporary workaround to compare exception message, until server provides better way of handling this case.
            if (clientException.Message.Contains("Reduce page size and try again."))
                return DocDbError.MaxItemCountTooLarge;

            return DocDbError.Undefined;
        }
    }
}