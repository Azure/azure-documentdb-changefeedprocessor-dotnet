//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging.LogProviders;

    internal class TraceLogProvider : LogProviderBase
    {
        private static readonly TraceSource TraceSource = new TraceSource("ChangeFeedEventHost");
        private static int traceId;
        private string prefix;

        public override Logger GetLogger(string name)
        {
            return this.Log;
        }

        public bool Log(LogLevel logLevel, Func<string> messageFunc, Exception exception, params object[] formatParameters)
        {
            if (messageFunc == null)
            {
                return true;
            }

            string message = string.Format(CultureInfo.InvariantCulture, "{0}: {1}: {2}", DateTime.UtcNow, this.prefix, messageFunc());
            IEnumerable<string> patternMatches;
            string formattedMessage =
                LogMessageFormatter.FormatStructuredMessage(
                    message,
                    formatParameters,
                    out patternMatches);

            var traceEventType = MapSeverity(logLevel);
            string logLine = exception == null ?
                    formattedMessage :
                    string.Concat(formattedMessage, Environment.NewLine, exception);
            TraceSource.TraceEvent(traceEventType, Interlocked.Increment(ref traceId), logLine);
            return true;
        }

        protected override OpenNdc GetOpenNdcMethod()
        {
            return this.OpenNdcMethod;
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

        private IDisposable OpenNdcMethod(string message)
        {
            if (!string.IsNullOrEmpty(this.prefix))
            {
                string logLine = $"Cannot open more than one nested log context. A context for '{this.prefix}' is already open. All trace calls for '{message}' will use '{this.prefix}'. Consider using different app domain.";
                TraceSource.TraceEvent(TraceEventType.Error, Interlocked.Increment(ref traceId), logLine);
            }
            else
            {
                this.prefix = message;
            }

            return new DisposableAction();
        }
    }
}
