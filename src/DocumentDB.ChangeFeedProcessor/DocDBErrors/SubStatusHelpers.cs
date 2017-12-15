using System.Globalization;
using Microsoft.Azure.Documents;

namespace DocumentDB.ChangeFeedProcessor.DocDBErrors
{
    internal static class SubStatusHelpers
    {
        public static SubStatusCode GetSubStatusCode(this DocumentClientException exception)
        {
            const string subStatusHeaderName = "x-ms-substatus";

            string valueSubStatus = exception.ResponseHeaders.Get(subStatusHeaderName);
            if (!string.IsNullOrEmpty(valueSubStatus))
            {
                int subStatusCode;
                if (int.TryParse(valueSubStatus, NumberStyles.Integer, CultureInfo.InvariantCulture, out subStatusCode))
                    return (SubStatusCode)subStatusCode;
            }
            return SubStatusCode.Undefined;
        }
    }
}