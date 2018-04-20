﻿//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
#pragma warning disable CS0618 // Type or member is obsolete

    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils;
    using Microsoft.Azure.Documents.Client;
    using Xunit;

    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public class EstimatorTests : IntegrationTest
    {
        public EstimatorTests(IntegrationTestFixture fixture) : base(fixture, typeof(EstimatorTests), false)
        {
        }

        [Fact]
        public async Task CoundPendingDocuments()
        {
            int documentCount = 1;
            int partitionCount = await IntegrationTestsHelper.GetPartitionCount(this.ClassData.monitoredCollectionInfo);
            int openedCount = 0, processedCount = 0;
            var allObserversStarted = new ManualResetEvent(false);
            var allDocsProcessed = new ManualResetEvent(false);

            var observerFactory = new TestObserverFactory(
                context =>
                {
                    int newCount = Interlocked.Increment(ref openedCount);
                    if (newCount == partitionCount) allObserversStarted.Set();
                    return Task.CompletedTask;
                },
                null,
                (ChangeFeedObserverContext context, IReadOnlyList<Document> docs) =>
                {
                    int newCount = Interlocked.Add(ref processedCount, docs.Count);
                    if (newCount == documentCount) allDocsProcessed.Set();
                    return Task.CompletedTask;
                });

            var hostName = Guid.NewGuid().ToString();

            var host = new ChangeFeedEventHost(
                hostName,
                this.ClassData.monitoredCollectionInfo,
                this.LeaseCollectionInfo,
                new ChangeFeedOptions { StartFromBeginning = false },
                new ChangeFeedHostOptions());

            // Initialize leases
            await host.RegisterObserverFactoryAsync(observerFactory);

            // Verify that 0 is returned on empty collection
            long estimation = await host.GetEstimatedRemainingWork();
            Assert.Equal(0, estimation);

            using (var client = new DocumentClient(this.ClassData.monitoredCollectionInfo.Uri, this.ClassData.monitoredCollectionInfo.MasterKey, this.ClassData.monitoredCollectionInfo.ConnectionPolicy))
            {
                await IntegrationTestsHelper.CreateDocumentsAsync(
                    client,
                    UriFactory.CreateDocumentCollectionUri(this.ClassData.monitoredCollectionInfo.DatabaseName, this.ClassData.monitoredCollectionInfo.CollectionName),
                    1);

                var isStartOk = allObserversStarted.WaitOne(IntegrationTest.changeWaitTimeout + IntegrationTest.changeWaitTimeout);
                Assert.True(isStartOk, "Timed out waiting for observer to start");

                allDocsProcessed.WaitOne(IntegrationTest.changeWaitTimeout);

                // Halt the processor temporarily
                await host.UnregisterObserversAsync();

                estimation = await host.GetEstimatedRemainingWork();
                Assert.Equal(0, estimation);

                await IntegrationTestsHelper.CreateDocumentsAsync(
                    client,
                    UriFactory.CreateDocumentCollectionUri(this.ClassData.monitoredCollectionInfo.DatabaseName, this.ClassData.monitoredCollectionInfo.CollectionName),
                    1);

                estimation = await host.GetEstimatedRemainingWork();
                Assert.Equal(1, estimation);

                await IntegrationTestsHelper.CreateDocumentsAsync(
                    client,
                    UriFactory.CreateDocumentCollectionUri(this.ClassData.monitoredCollectionInfo.DatabaseName, this.ClassData.monitoredCollectionInfo.CollectionName),
                    10);

                estimation = await host.GetEstimatedRemainingWork();
                Assert.Equal(11, estimation);

                // Create a new host to process pending changes
                var newHost = new ChangeFeedEventHost(
                    hostName,
                    this.ClassData.monitoredCollectionInfo,
                    this.LeaseCollectionInfo,
                    new ChangeFeedOptions { StartFromBeginning = false },
                    new ChangeFeedHostOptions());

                openedCount = 0;
                processedCount = 0;
                allObserversStarted.Reset();
                allDocsProcessed.Reset();

                await newHost.RegisterObserverFactoryAsync(observerFactory);
                isStartOk = allObserversStarted.WaitOne(IntegrationTest.changeWaitTimeout + IntegrationTest.changeWaitTimeout);
                Assert.True(isStartOk, "Timed out waiting for observer to start");

                allDocsProcessed.WaitOne(IntegrationTest.changeWaitTimeout);

                try
                {
                    estimation = await newHost.GetEstimatedRemainingWork();
                    Assert.Equal(0, estimation);
                }
                finally
                {
                    await newHost.UnregisterObserversAsync();
                }
            }
        }
    }
}
