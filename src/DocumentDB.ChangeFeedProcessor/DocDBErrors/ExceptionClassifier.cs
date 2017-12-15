using System.Net;
using Microsoft.Azure.Documents;

namespace DocumentDB.ChangeFeedProcessor.DocDBErrors
{
    internal static class ExceptionClassifier
    {
        public static DocDbError ClassifyClientException(DocumentClientException clientException)
        {
            SubStatusCode subStatusCode = clientException.GetSubStatusCode();

            if (clientException.StatusCode == HttpStatusCode.NotFound && SubStatusCode.ReadSessionNotAvailable != subStatusCode)
                return DocDbError.PartitionNotFound;

            if (clientException.StatusCode == HttpStatusCode.Gone && (SubStatusCode.PartitionKeyRangeGone == subStatusCode || SubStatusCode.Splitting == subStatusCode))
                return DocDbError.PartitionSplit;

            if (clientException.StatusCode == (HttpStatusCode)429 || clientException.StatusCode >= HttpStatusCode.InternalServerError)
                return DocDbError.TransientError;

            return DocDbError.Undefined;
        }
    }
}