using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    public class ChangeFeedEventHost
    {
        private readonly ChangeFeedHostBuilder builder = new ChangeFeedHostBuilder();
        private IChangeFeedHost host;

        static ChangeFeedEventHost()
        {
            var traceLogProvider = new Tuple<LogProvider.IsLoggerAvailable, LogProvider.CreateLogProvider>(() => true, () => new TraceLogProvider());
            LogProvider.LogProviderResolvers.Add(traceLogProvider);
        }

        public ChangeFeedEventHost(string hostName,
                                   DocumentCollectionInfo feedCollectionLocation,
                                   DocumentCollectionInfo leaseCollectionLocation,
                                   ChangeFeedHostOptions changeFeedHostOptions)
        {
            if (string.IsNullOrEmpty(hostName))
                throw new ArgumentNullException(nameof(hostName));
            if (feedCollectionLocation == null)
                throw new ArgumentNullException(nameof(feedCollectionLocation));
            if (leaseCollectionLocation == null)
                throw new ArgumentNullException(nameof(leaseCollectionLocation));
            if (changeFeedHostOptions == null)
                throw new ArgumentNullException(nameof(changeFeedHostOptions));

            builder
                .WithHostName(hostName)
                .WithFeedCollection(feedCollectionLocation)
                .WithChangeFeedHostOptions(changeFeedHostOptions)
                .WithPartitionManagerBuilder(
                    new PartitionManagerBuilder()
                        .WithLeaseCollection(leaseCollectionLocation));
        }

        /// <summary>Asynchronously registers the observer interface implementation with the host.
        /// This method also starts the host and enables it to start participating in the partition distribution process.</summary>
        /// <typeparam name="T">Implementation of your application-specific event observer.</typeparam>
        /// <returns>A task indicating that the <see cref="DocumentDB.ChangeFeedProcessor.ChangeFeedEventHost" /> instance has started.</returns>
        public async Task RegisterObserverAsync<T>() where T : IChangeFeedObserver, new()
        {
            builder.WithObserver<T>();
            host = await builder.BuildAsync().ConfigureAwait(false);
            await host.StartAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Asynchronously registers the observer factory implementation with the host.
        /// This method also starts the host and enables it to start participating in the partition distribution process.
        /// </summary>
        /// <param name="factory">Implementation of your application-specific event observer factory.</param>
        /// <returns>A task indicating that the <see cref="DocumentDB.ChangeFeedProcessor.ChangeFeedEventHost" /> instance has started.</returns>
        public async Task RegisterObserverFactoryAsync(IChangeFeedObserverFactory factory)
        {
            builder.WithObserverFactory(factory);
            host = await builder.BuildAsync().ConfigureAwait(false);
            await host.StartAsync().ConfigureAwait(false);
        }

        /// <summary>Asynchronously shuts down the host instance. This method maintains the leases on all partitions currently held, and enables each 
        /// host instance to shut down cleanly by invoking the method with object.</summary> 
        /// <returns>A task that indicates the host instance has stopped.</returns>
        public async Task UnregisterObserversAsync()
        {
            await host.StopAsync().ConfigureAwait(false);
        }
    }
}