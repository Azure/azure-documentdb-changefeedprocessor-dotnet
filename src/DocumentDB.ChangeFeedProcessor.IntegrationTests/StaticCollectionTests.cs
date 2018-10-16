//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils;
using Microsoft.Azure.Documents.Client;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
#pragma warning disable CS0618 // Type or member is obsolete

    /// <summary>
    /// For these tests, the collection is not modified while tests are running.
    /// </summary>
    /// <remarks>
    /// The class is abstract to avoid xunit discovering its tests, the tests must be discovered from derived classes only.
    /// </remarks>
    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public abstract class StaticCollectionTests : IntegrationTest
    {
        const int documentCount = 1519;

        public StaticCollectionTests(
            IntegrationTestFixture fixture,
            Type testClassType,
            bool isPartitionedLeaseCollection) : 
            base(fixture, testClassType, isPartitionedLeaseCollection: isPartitionedLeaseCollection)
        {
        }

        [Fact]
        public async Task CountDocumentsInCollection_NormalCase()
        {
            int partitionKeyRangeCount = await IntegrationTestsHelper.GetPartitionCount(this.ClassData.monitoredCollectionInfo);
            int openedCount = 0, closedCount = 0, processedCount = 0;
            var allDocsProcessed = new ManualResetEvent(false);

            var observerFactory = new TestObserverFactory(
                context => { Interlocked.Increment(ref openedCount); return Task.CompletedTask; },
                (context, reason) => { Interlocked.Increment(ref closedCount); return Task.CompletedTask; },
                (ChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
                {
                    int newCount = Interlocked.Add(ref processedCount, docs.Count);
                    if (newCount == documentCount)
                    {
                        allDocsProcessed.Set();
                    }
                    return Task.CompletedTask;
                });

            var host = new ChangeFeedEventHost(
                Guid.NewGuid().ToString(),
                this.ClassData.monitoredCollectionInfo,
                this.LeaseCollectionInfo,
                new ChangeFeedOptions { StartFromBeginning = true },
                new ChangeFeedHostOptions());
            await host.RegisterObserverFactoryAsync(observerFactory);

            allDocsProcessed.WaitOne(changeWaitTimeout + changeWaitTimeout);

            try
            {
                Assert.True(partitionKeyRangeCount == openedCount, "Wrong openedCount");
                Assert.True(documentCount == processedCount, "Wrong processedCount");
            }
            finally
            {
                await host.UnregisterObserversAsync();
            }

            Assert.True(partitionKeyRangeCount == closedCount, "Wrong closedCount");
        }

        [Fact]
        public async Task CountDocumentsInCollection_ProcessChangesThrows()
        {
            int processedCount = 0;
            var allDocsProcessed = new ManualResetEvent(false);
            bool isFirstChangeNotification = false; // Make sure there was at least one throw.
            int throwCount = 0;

            var observerFactory = new TestObserverFactory((ChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
            {
                bool shouldThrow = (isFirstChangeNotification || new Random().Next(0, 1) == 1) && throwCount < 10;
                isFirstChangeNotification = false;

                if (shouldThrow)
                {
                    Interlocked.Increment(ref throwCount);
                    throw new Exception("Error injection exception from observer!");
                }

                int newCount = Interlocked.Add(ref processedCount, docs.Count);
                if (newCount == documentCount)
                {
                    allDocsProcessed.Set();
                }
                return Task.CompletedTask;
            });

            var host = new ChangeFeedEventHost(
                Guid.NewGuid().ToString(),
                this.ClassData.monitoredCollectionInfo,
                this.LeaseCollectionInfo,
                new ChangeFeedOptions { StartFromBeginning = true },
                new ChangeFeedHostOptions());
            await host.RegisterObserverFactoryAsync(observerFactory);

            allDocsProcessed.WaitOne(changeWaitTimeout + changeWaitTimeout + changeWaitTimeout);

            try
            {
                Assert.Equal(documentCount, processedCount);
            }
            finally
            {
                await host.UnregisterObserversAsync();
            }
        }

        [Fact]
        public async Task CountDocumentsInCollection_TwoHosts()
        {
            int partitionKeyRangeCount = await IntegrationTestsHelper.GetPartitionCount(this.ClassData.monitoredCollectionInfo);
            Assert.True(partitionKeyRangeCount > 1, "Prerequisite failed: expected monitored collection with at least 2 partitions.");

            int processedCount = 0;
            var allDocsProcessed = new ManualResetEvent(false);

            var observerFactory = new TestObserverFactory(
                (ChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
                {
                    int newCount = Interlocked.Add(ref processedCount, docs.Count);
                    if (newCount == documentCount)
                    {
                        allDocsProcessed.Set();
                    }
                    return Task.CompletedTask;
                });

            var host1 = new ChangeFeedEventHost(
                Guid.NewGuid().ToString(),
                this.ClassData.monitoredCollectionInfo,
                this.LeaseCollectionInfo,
                new ChangeFeedOptions { StartFromBeginning = true },
                new ChangeFeedHostOptions { MaxPartitionCount = partitionKeyRangeCount / 2 });
            await host1.RegisterObserverFactoryAsync(observerFactory);

            var host2 = new ChangeFeedEventHost(
                Guid.NewGuid().ToString(),
                this.ClassData.monitoredCollectionInfo,
                this.LeaseCollectionInfo,
                new ChangeFeedOptions { StartFromBeginning = true },
                new ChangeFeedHostOptions { MaxPartitionCount = partitionKeyRangeCount - partitionKeyRangeCount / 2 });
            await host2.RegisterObserverFactoryAsync(observerFactory);

            allDocsProcessed.WaitOne(changeWaitTimeout + changeWaitTimeout);

            try
            {
                Assert.True(documentCount == processedCount, "Wrong processedCount");
            }
            finally
            {
                await host1.UnregisterObserversAsync();
                await host2.UnregisterObserversAsync();
            }
        }

        [Fact]
        public async Task StopAtFullSpeed()
        {
            int partitionKeyRangeCount = await IntegrationTestsHelper.GetPartitionCount(this.ClassData.monitoredCollectionInfo);
            int openedCount = 0, closedCount = 0, processedCount = 0;
            var quarterDocsProcessed = new ManualResetEvent(false);

            var observerFactory = new TestObserverFactory(
                context => { Interlocked.Increment(ref openedCount); return Task.CompletedTask; },
                (context, reason) => { Interlocked.Increment(ref closedCount); return Task.CompletedTask; },
                (ChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
                {
                    int newCount = Interlocked.Add(ref processedCount, docs.Count);
                    if (newCount >= documentCount / 4)
                    {
                        quarterDocsProcessed.Set();
                    }
                    return Task.CompletedTask;
                });

            var host = new ChangeFeedEventHost(
                Guid.NewGuid().ToString(),
                this.ClassData.monitoredCollectionInfo,
                this.LeaseCollectionInfo,
                new ChangeFeedOptions { StartFromBeginning = true, MaxItemCount = 2 },
                new ChangeFeedHostOptions());
            await host.RegisterObserverFactoryAsync(observerFactory);

            quarterDocsProcessed.WaitOne(changeWaitTimeout + changeWaitTimeout);

            await host.UnregisterObserversAsync();

            Assert.True(partitionKeyRangeCount == openedCount, "Wrong closedCount");
            Assert.True(partitionKeyRangeCount == closedCount, "Wrong closedCount");
        }

        protected override async Task FinishTestClassInitializeAsync()
        {
            using (var client = new DocumentClient(this.ClassData.monitoredCollectionInfo.Uri, this.ClassData.monitoredCollectionInfo.MasterKey, this.ClassData.monitoredCollectionInfo.ConnectionPolicy))
            {
                var collectionUri = UriFactory.CreateDocumentCollectionUri(this.ClassData.monitoredCollectionInfo.DatabaseName, this.ClassData.monitoredCollectionInfo.CollectionName);
                await IntegrationTestsHelper.CreateDocumentsAsync(client, collectionUri, documentCount);
            }
        }
    }
}
