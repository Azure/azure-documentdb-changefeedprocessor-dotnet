//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------
namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Logging
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using System.Diagnostics;
    using System.IO;
    using Xunit;
    using Xunit.Abstractions;

    [Trait("Category", "Gated")]
    public class TraceLogProviderTests
    {
        [Fact]
        public void Log_WhenTraceProviderConstructedWithTraceSource()
        {
            using (Stream stream = new MemoryStream(128))
            {
                TraceSource traceSource = new TraceSource("CFP", SourceLevels.All);
                traceSource.Listeners.Clear();
                traceSource.Listeners.Add(new TextWriterTraceListener(stream));

                TraceLogProvider tracelogProvider = new TraceLogProvider(traceSource);
                LogProvider.SetCurrentLogProvider(tracelogProvider);

                ILog logger = LogProvider.GetCurrentClassLogger();
                logger.Info("test trace");

                traceSource.Flush();
                stream.Seek(0, SeekOrigin.Begin);
                var traceOutput = new StreamReader(stream).ReadToEnd();

                Assert.Contains("test trace", traceOutput);
            }
        }
    }
}
