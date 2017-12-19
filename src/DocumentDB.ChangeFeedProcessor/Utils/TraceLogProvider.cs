//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging.LogProviders;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Utils
{
    internal class TraceLogProvider : LogProviderBase
    {
        private static readonly TraceSource traceSource = new TraceSource("ChangeFeedEventHost");
        private static int traceId;

        public override Logger GetLogger(string name)
        {
            return Log;
        }

        private static TraceEventType MapSeverity(LogLevel logLevel)
        {
            switch (logLevel)
            {
                case LogLevel.Fatal:
                    return TraceEventType.Critical;
                case LogLevel.Error:
                    return TraceEventType.Error;
                case LogLevel.Warn:
                    return TraceEventType.Warning;
                case LogLevel.Info:
                    return TraceEventType.Information;
                default:
                    return TraceEventType.Verbose;
            }
        }

        public bool Log(LogLevel logLevel, Func<string> messageFunc, Exception exception, params object[] formatParameters)
        {
            if (messageFunc == null)
            {
                return true;
            }

            string message = messageFunc();
            IEnumerable<string> patternMatches;
            string formattedMessage =
                LogMessageFormatter.FormatStructuredMessage(message,
                    formatParameters,
                    out patternMatches);

            var traceEventType = MapSeverity(logLevel);
            string logLine = 
                exception == null ?
                    formattedMessage :
                    string.Concat(formattedMessage, Environment.NewLine, exception);
            traceSource.TraceEvent(traceEventType, Interlocked.Increment(ref traceId),
                logLine);
            return true;
        }
    }
}
