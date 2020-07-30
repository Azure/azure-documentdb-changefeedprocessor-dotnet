//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests.Utils;
using Microsoft.Azure.Documents.Client;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.IntegrationTests
{
    /// <summary>
    /// Fixture is shared among all instances. https://xunit.github.io/docs/shared-context.html#collection-fixture
    /// </summary>
    public class IntegrationTestFixture : IDisposable
    {
        public IntegrationTestFixture()
        {
            System.Net.ServicePointManager.DefaultConnectionLimit = 1000;  // Default is 2.
            ThreadPool.SetMinThreads(1000, 1000);   // 32
            ThreadPool.SetMaxThreads(5000, 5000);   // 32
        }

        public void Dispose()
        {
            DisposeAsync().Wait();
        }

        public async Task DisposeAsync()
        {
            IntegrationTestsHelper.GetConfigurationSettings(out string endpointAsString, 
                out string masterKey, 
                out string databaseName);

            var endpoint = new Uri(endpointAsString);

            using (var client = new DocumentClient(endpoint, masterKey))
            {
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(databaseName));
            }
        }
    }

    [CollectionDefinition("Integration tests")]
    public class IntegrationTestCollection : ICollectionFixture<IntegrationTestFixture>
    {
    }

    /// <summary>
    /// Base class for intergration tests.
    /// Serves the following:
    /// - Per derived test class, initialize/cleanup of the monitored collection. Each derived test class gets different monitored collection.
    ///   When using "run all tests", the collection used by each derived test class is cleaned up when last test of the class finishes, not later.
    /// - Per test method, initialize/cleanup of lease collection.
    /// - Each derived class needs to have a constructor that receives the fixture and passes it to base along with its typeof. For example:
    ///      public DynamicCollectionTests(IntegrationTestFixture fixture): base(fixture, typeof(DynamicCollectionTests))
    /// - Each derived class needs to decorate with [Trait("Category", "Integration")] and [Collection("Integration tests")]
    /// </summary>
    /// <remarks>
    /// Test method instance is not preserved across tests in same test class, can't share state in test class instance across test methods.
    /// </remarks>
    [Trait("Category", "Integration")]
    [Collection("Integration tests")]
    public class IntegrationTest: IAsyncLifetime
    {
        protected static int monitoredOfferThroughput;
        protected static int leaseOfferThroughput;
        protected static readonly TimeSpan changeWaitTimeout = TimeSpan.FromSeconds(30);

        IntegrationTestFixture fixture;

        protected DocumentCollectionInfo LeaseCollectionInfo
        {
            get;
            private set;
        }

        protected DocumentCollectionInfo MonitoredCollectionInfo
        {
            get;
            private set;
        }

        protected readonly bool IsPartitionedMonitoredCollection;

        protected readonly bool IsPartitionedLeaseCollection;

        public IntegrationTest(
            bool isPartitionedMonitoredCollection = true,
            bool isPartitionedLeaseCollection = false)
        {
            this.IsPartitionedMonitoredCollection = isPartitionedMonitoredCollection;
            this.IsPartitionedLeaseCollection = isPartitionedLeaseCollection;
        }

        public async Task InitializeAsync()
        {
            try
            {
                await this.CreateMonitoredCollectionAsync($"data_{this.GetType().Name}");
            }
            catch(Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }

            this.LeaseCollectionInfo.CollectionName = $"leases_{this.GetType().Name}_{Guid.NewGuid().ToString()}";

            var leaseCollection = new DocumentCollection
            {
                Id = this.LeaseCollectionInfo.CollectionName,
            };

            if (this.IsPartitionedLeaseCollection)
            {
                leaseCollection.PartitionKey = new PartitionKeyDefinition { Paths = { "/id" } };
            }

            using (var client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
            {
                await IntegrationTestsHelper.CreateDocumentCollectionAsync(client, this.LeaseCollectionInfo.DatabaseName, leaseCollection, leaseOfferThroughput);
            }
        }

        public async Task DisposeAsync()
        {
            Debug.Assert(this.LeaseCollectionInfo != null);
            using (var client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
            {
                await client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(this.LeaseCollectionInfo.DatabaseName, this.LeaseCollectionInfo.CollectionName));
                await client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(this.MonitoredCollectionInfo.DatabaseName, this.MonitoredCollectionInfo.CollectionName));
            }
        }

        private async Task CreateMonitoredCollectionAsync(string monitoredCollectionName)
        {
            Debug.Assert(monitoredCollectionName != null);

            IntegrationTestsHelper.GetConfigurationSettings(
                out DocumentCollectionInfo baseMonitoredCollectionInfo,
                out DocumentCollectionInfo baseLeaseCollectionInfo,
                out monitoredOfferThroughput,
                out leaseOfferThroughput);

            this.MonitoredCollectionInfo = baseMonitoredCollectionInfo;
            this.LeaseCollectionInfo = baseLeaseCollectionInfo;

            this.MonitoredCollectionInfo.CollectionName = monitoredCollectionName;

            var monitoredCollection = new DocumentCollection
            {
                Id = this.MonitoredCollectionInfo.CollectionName,
            };

            if (this.IsPartitionedMonitoredCollection)
            {
                monitoredCollection.PartitionKey = new PartitionKeyDefinition { Paths = { "/id" } };
            }
            else
            {
                if (monitoredOfferThroughput > 10000)
                {
                    monitoredOfferThroughput = 10000;
                }
            }

            using (var client = new DocumentClient(this.MonitoredCollectionInfo.Uri, this.MonitoredCollectionInfo.MasterKey, this.MonitoredCollectionInfo.ConnectionPolicy))
            {
                await IntegrationTestsHelper.CreateDocumentCollectionAsync(client, this.MonitoredCollectionInfo.DatabaseName, monitoredCollection, monitoredOfferThroughput);
            }
        }
    }
}