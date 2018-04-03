//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Logging
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Threading;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging.LogProviders;

    /// <summary>
    /// Implements a trace log provider for .Net tracing.
    /// </summary>
    /// <example>
    /// <code language="C#">
    /// <![CDATA[
    /// using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    ///
    /// var hostName = "SampleHost";
    /// var tracelogProvider = new TraceLogProvider();
    /// using (tracelogProvider.OpenNestedContext(hostName))
    /// {
    ///     LogProvider.SetCurrentLogProvider(tracelogProvider);
    ///
    ///     // After this, create IChangeFeedProcessor instance and start/stop it.
    /// }
    /// ]]>
    /// </code>
    /// </example>
    public class TraceLogProvider : ILogProvider
    {
        private TraceLogProviderImplementation logProvider = new TraceLogProviderImplementation();

        /// <summary>
        /// Initializes a new instance of the <see cref="TraceLogProvider"/> class.
        /// </summary>
        public TraceLogProvider()
        {
        }

        /// <summary>
        /// Gets the logger for this log provider.
        /// </summary>
        /// <param name="name">The name of the logger.</param>
        /// <returns>The <see cref="Logger"/> instance used by this log provier.</returns>
        public Logger GetLogger(string name)
        {
            return this.logProvider.GetLogger(name);
        }

        /// <summary>
        /// Opens nested context.
        /// </summary>
        /// <param name="message">The message to pass to the nested context.</param>
        /// <returns>An instance of <see cref="IDisposable"/> to dispose when the context is no longer needed.</returns>
        public IDisposable OpenNestedContext(string message)
        {
            return this.logProvider.OpenNestedContext(message);
        }

        /// <summary>
        /// Opens mapped context.
        /// </summary>
        /// <param name="key">The key to pass to the nested context.</param>
        /// <param name="value">The value to pass to the nested context.</param>
        /// <returns>An instance of <see cref="IDisposable"/> to dispose when the context is no longer needed.</returns>
        public IDisposable OpenMappedContext(string key, string value)
        {
            return this.logProvider.OpenMappedContext(key, value);
        }

        internal class TraceLogProviderImplementation : LogProviderBase
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
}
