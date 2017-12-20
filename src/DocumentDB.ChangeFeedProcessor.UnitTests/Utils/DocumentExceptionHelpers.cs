//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Net.Http.Headers;
using System.Reflection;
using Microsoft.Azure.Documents;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Utils
{
    public static class DocumentExceptionHelpers
    {
        public static Exception CreateNotFoundException()
        {
            return CreateException("Microsoft.Azure.Documents.NotFoundException", 1);
        }
        public static Exception CreatePreconditionFailedException()
        {
            return CreateException("Microsoft.Azure.Documents.PreconditionFailedException", 1);
        }
        public static Exception CreateConflictException()
        {
            return CreateException("Microsoft.Azure.Documents.ConflictException", 1);
        }

        public static Exception CreateRequestRateTooLargeException()
        {
            return CreateException("Microsoft.Azure.Documents.RequestRateTooLargeException", 1);
        }

        public static Exception CreateException(string exceptionType, int subStatusCode)
        {
            Type t = typeof(DocumentClientException)
                .Assembly.GetType(exceptionType);

            HttpResponseHeaders httpResponseHeaders = CreateResponseHeaders();
            httpResponseHeaders.TryAddWithoutValidation("x-ms-substatus", subStatusCode.ToString());
            httpResponseHeaders.TryAddWithoutValidation("x-ms-activity-id", "activityId");

            object ex = t.GetConstructor(
                BindingFlags.Public | BindingFlags.Instance,
                null, new[] { typeof(string), typeof(HttpResponseHeaders), typeof(Uri) }, null).Invoke(new object[] { "", httpResponseHeaders, null });
            return ex as Exception;
        }

        private static HttpResponseHeaders CreateResponseHeaders()
        {
            object o = typeof(HttpResponseHeaders).GetConstructor(
                BindingFlags.NonPublic | BindingFlags.Instance,
                null, new Type[] { }, null).Invoke(null);
            return o as HttpResponseHeaders;
        }
    }
}