//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;
using Microsoft.Azure.Documents.Client;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    /// <summary>
    /// Simple host for distributing change feed events across observers and thus allowing these observers scale.
    /// It distributes the load across its instances and allows dynamic scaling:
    ///   - Partitions in partitioned collections are distributed across instances/observers.
    ///   - New instance takes leases from existing instances to make distribution equal.
    ///   - If an instance dies, the leases are distributed across remaining instances.
    /// It's useful for scenario when partition count is high so that one host/VM is not capable of processing that many change feed events.
    /// Client application needs to implement <see cref="Microsoft.Azure.Documents.ChangeFeedProcessor.IChangeFeedObserver"/> and register processor implementation with ChangeFeedEventHost.
    /// </summary>
    /// <remarks>
    /// It uses auxiliary document collection for managing leases for a partition.
    /// Every EventProcessorHost instance is performing the following two tasks:
    ///     1) Renew Leases: It keeps track of leases currently owned by the host and continuously keeps on renewing the leases.
    ///     2) Acquire Leases: Each instance continuously polls all leases to check if there are any leases it should acquire 
    ///     for the system to get into balanced state.
    /// </remarks>
    /// <example>
    /// <code language="c#">
    /// <![CDATA[
    /// class DocumentFeedObserver : IChangeFeedObserver
    /// {
    ///     private static int s_totalDocs = 0;
    ///     public Task OpenAsync(ChangeFeedObserverContext context)
    ///     {
    ///         Console.WriteLine("Worker opened, {0}", context.PartitionKeyRangeId);
    ///         return Task.CompletedTask;  // Requires targeting .NET 4.6+.
    ///     }
    ///     public Task CloseAsync(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
    ///     {
    ///         Console.WriteLine("Worker closed, {0}", context.PartitionKeyRangeId);
    ///         return Task.CompletedTask;
    ///     }
    ///     public Task ProcessChangesAsync(ChangeFeedObserverContext context, IReadOnlyList<Document> docs)
    ///     {
    ///         Console.WriteLine("Change feed: total {0} doc(s)", Interlocked.Add(ref s_totalDocs, docs.Count));
    ///         return Task.CompletedTask;
    ///     }
    /// }
    /// static async Task StartChangeFeedHost()
    /// {
    ///     string hostName = Guid.NewGuid().ToString();
    ///     DocumentCollectionInfo documentCollectionLocation = new DocumentCollectionInfo
    ///     {
    ///         Uri = new Uri("https://YOUR_SERVICE.documents.azure.com:443/"),
    ///         MasterKey = "YOUR_SECRET_KEY==",
    ///         DatabaseName = "db1",
    ///         CollectionName = "documents"
    ///     };
    ///     DocumentCollectionInfo leaseCollectionLocation = new DocumentCollectionInfo
    ///     {
    ///         Uri = new Uri("https://YOUR_SERVICE.documents.azure.com:443/"),
    ///         MasterKey = "YOUR_SECRET_KEY==",
    ///         DatabaseName = "db1",
    ///         CollectionName = "leases"
    ///     };
    ///     Console.WriteLine("Main program: Creating ChangeFeedEventHost...");
    ///     ChangeFeedEventHost host = new ChangeFeedEventHost(hostName, documentCollectionLocation, leaseCollectionLocation);
    ///     await host.RegisterObserverAsync<DocumentFeedObserver>();
    ///     Console.WriteLine("Main program: press Enter to stop...");
    ///     Console.ReadLine();
    ///     await host.UnregisterObserversAsync();
    /// }
    /// ]]>
    /// </code>
    /// </example>
    public class ChangeFeedEventHost
    {
        private readonly ChangeFeedHostBuilder builder = new ChangeFeedHostBuilder();
        private IChangeFeedHost host;

        static ChangeFeedEventHost()
        {
            var traceLogProvider = new Tuple<LogProvider.IsLoggerAvailable, LogProvider.CreateLogProvider>(() => true, () => new TraceLogProvider());
            LogProvider.LogProviderResolvers.Add(traceLogProvider);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Microsoft.Azure.Documents.ChangeFeedProcessor.ChangeFeedEventHost"/> class.
        /// </summary>
        /// <param name="hostName">Unique name for this host.</param>
        /// <param name="documentCollectionLocation">Specifies location of the DocumentDB collection to monitor changes for.</param>
        /// <param name="auxCollectionLocation">Specifies location of auxiliary data for load-balancing instances of <see cref="Microsoft.Azure.Documents.ChangeFeedProcessor.ChangeFeedEventHost" />.</param>
        public ChangeFeedEventHost(string hostName, DocumentCollectionInfo documentCollectionLocation, DocumentCollectionInfo auxCollectionLocation)
            : this(hostName, documentCollectionLocation, auxCollectionLocation, new ChangeFeedOptions(), new ChangeFeedHostOptions())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ChangeFeedEventHost"/> class.
        /// </summary>
        /// <param name="hostName">Unique name for this host.</param>
        /// <param name="feedCollectionLocation">Specifies location of the Cosmos DB collection to monitor changes for.</param>
        /// <param name="leaseCollectionLocation">Specifies location of auxiliary data for load-balancing instances of <see cref="ChangeFeedEventHost" />.</param>
        /// <param name="changeFeedHostOptions">Additional options to control load-balancing of <see cref="ChangeFeedEventHost" /> instances.</param>
        public ChangeFeedEventHost(string hostName,
                                   DocumentCollectionInfo feedCollectionLocation,
                                   DocumentCollectionInfo leaseCollectionLocation,
                                   ChangeFeedHostOptions changeFeedHostOptions) 
            : this(hostName, feedCollectionLocation, leaseCollectionLocation, new ChangeFeedOptions(), changeFeedHostOptions)
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ChangeFeedEventHost"/> class.
        /// </summary>
        /// <param name="hostName">Unique name for this host.</param>
        /// <param name="feedCollectionLocation">Specifies location of the Cosmos DB collection to monitor changes for.</param>
        /// <param name="leaseCollectionLocation">Specifies location of auxiliary data for load-balancing instances of <see cref="ChangeFeedEventHost" />.</param>
        /// <param name="changeFeedOptions">Options to pass to the <see cref="DocumentClient.CreateDocumentChangeFeedQuery" /> API.</param>
        /// <param name="changeFeedHostOptions">Additional options to control load-balancing of <see cref="ChangeFeedEventHost" /> instances.</param>
        public ChangeFeedEventHost(string hostName,
                                   DocumentCollectionInfo feedCollectionLocation,
                                   DocumentCollectionInfo leaseCollectionLocation,
                                   ChangeFeedOptions changeFeedOptions,
                                   ChangeFeedHostOptions changeFeedHostOptions)
        {
            if (string.IsNullOrEmpty(hostName))
                throw new ArgumentNullException(nameof(hostName));
            if (feedCollectionLocation == null)
                throw new ArgumentNullException(nameof(feedCollectionLocation));
            if (leaseCollectionLocation == null)
                throw new ArgumentNullException(nameof(leaseCollectionLocation));
            if (changeFeedOptions == null)
                throw new ArgumentNullException(nameof(changeFeedOptions));
            if (changeFeedHostOptions == null)
                throw new ArgumentNullException(nameof(changeFeedHostOptions));

            builder
                .WithHostName(hostName)
                .WithFeedCollection(feedCollectionLocation)
                .WithChangeFeedHostOptions(changeFeedHostOptions)
                .WithChangeFeedOptions(changeFeedOptions)
                .WithPartitionManagerBuilder(
                    new PartitionManagerBuilder()
                        .WithLeaseCollection(leaseCollectionLocation));
        }

        /// <summary>Asynchronously registers the observer interface implementation with the host.
        /// This method also starts the host and enables it to start participating in the partition distribution process.</summary>
        /// <typeparam name="T">Implementation of your application-specific event observer.</typeparam>
        /// <returns>A task indicating that the <see cref="ChangeFeedEventHost" /> instance has started.</returns>
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
        /// <returns>A task indicating that the <see cref="ChangeFeedEventHost" /> instance has started.</returns>
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