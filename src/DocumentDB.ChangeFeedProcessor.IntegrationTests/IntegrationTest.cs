//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
    public class TestClassData
    {
        internal readonly SemaphoreSlim classInitializeSyncRoot = new SemaphoreSlim(1, 1);
        internal readonly object testContextSyncRoot = new object();
        internal readonly int testCount;
        internal readonly bool isPartitionedMonitoredCollection;
        internal readonly bool isPartitionedLeaseCollection;
        internal volatile int executedTestCount;
        internal DocumentCollectionInfo monitoredCollectionInfo;
        internal DocumentCollectionInfo leaseCollectionInfoTemplate;

        internal TestClassData(int testCount, bool isPartitionedMonitoredCollection, bool isPartitionedLeaseCollection)
        {
            this.testCount = testCount;
            this.isPartitionedMonitoredCollection = isPartitionedMonitoredCollection;
            this.isPartitionedLeaseCollection = isPartitionedLeaseCollection;
        }
    }

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
            Properties = new ConcurrentDictionary<string, object>();
            testClasses = new ConcurrentDictionary<string, TestClassData>();
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

        public IDictionary<string, object> Properties { get; private set; }
        public IDictionary<string, TestClassData> testClasses { get; private set; }
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
    public class IntegrationTest: IDisposable
    {
        private const string leaseCollectionInfoPropertyName = "leaseCollectionInfo";
        protected static int monitoredOfferThroughput;
        protected static int leaseOfferThroughput;
        protected static readonly TimeSpan changeWaitTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        /// This dictionary has one entry per derived class.
        /// </summary>
        private static object testClassesSyncRoot = new object();

        IntegrationTestFixture fixture;

        protected DocumentCollectionInfo LeaseCollectionInfo
        {
            get { return (DocumentCollectionInfo)this.fixture.Properties[leaseCollectionInfoPropertyName]; }
            set
            {
                lock (this.ClassData.testContextSyncRoot)
                {
                    this.fixture.Properties[leaseCollectionInfoPropertyName] = value;
                }
            }
        }

        public TestClassData ClassData
        {
            get { return this.fixture.testClasses[this.GetType().Name]; }
        }
        
        public IntegrationTest(
            IntegrationTestFixture fixture,
            Type testClassType,
            bool isPartitionedCollection = true,
            bool isPartitionedLeaseCollection = false)
        {
            this.fixture = fixture;
            if (!this.fixture.testClasses.ContainsKey(testClassType.Name))
            {
                this.fixture.testClasses[testClassType.Name] = new TestClassData(
                    GetTestCount(testClassType),
                    isPartitionedCollection,
                    isPartitionedLeaseCollection);
            }

            TestInitializeAsync().Wait();
        }

        public async Task TestInitializeAsync()
        {
            if (this.ClassData.monitoredCollectionInfo == null)
            {
                try
                {
                    if (this.ClassData.monitoredCollectionInfo == null)
                    {
                        this.ClassData.leaseCollectionInfoTemplate = await TestClassInitializeAsync(this, $"data_{this.GetType().Name}");
                    }
                }
                catch(Exception ex)
                {
                    Debug.Write(ex);
                    throw;
                }
            }

            this.LeaseCollectionInfo = new DocumentCollectionInfo(this.ClassData.leaseCollectionInfoTemplate);
            this.LeaseCollectionInfo.CollectionName = $"leases_{this.GetType().Name}_{Guid.NewGuid().ToString()}";

            var leaseCollection = new DocumentCollection
            {
                Id = this.LeaseCollectionInfo.CollectionName,
            };

            if (this.ClassData.isPartitionedLeaseCollection)
            {
                leaseCollection.PartitionKey = new PartitionKeyDefinition { Paths = { "/id" } };
            }

            using (var client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
            {
                await IntegrationTestsHelper.CreateDocumentCollectionAsync(client, this.LeaseCollectionInfo.DatabaseName, leaseCollection, leaseOfferThroughput);
            }
        }

        public void Dispose()
        {
            TestCleanupAsync().Wait();
        }

        public async Task TestCleanupAsync()
        {
            Debug.Assert(this.LeaseCollectionInfo != null);
            using (var client = new DocumentClient(this.LeaseCollectionInfo.Uri, this.LeaseCollectionInfo.MasterKey, this.LeaseCollectionInfo.ConnectionPolicy))
            {
                await client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(this.LeaseCollectionInfo.DatabaseName, this.LeaseCollectionInfo.CollectionName));
            }

            var executedTestCount = Interlocked.Increment(ref this.ClassData.executedTestCount);
            if (this.ClassData.executedTestCount == this.ClassData.testCount)
            {
                await TestClassCleanupAsync(this);
            }
        }

        /// <summary>
        /// Recreates the test collection
        /// </summary>
        /// <returns></returns>
        public async Task ResetTestCollection()
        {
            await IntegrationTest.TestClassCleanupAsync(this);
            await IntegrationTest.TestClassInitializeAsync(this, $"data_{this.GetType().Name}");
        }

        protected virtual Task FinishTestClassInitializeAsync()
        {
            return Task.CompletedTask;
        }

        private static async Task<DocumentCollectionInfo> TestClassInitializeAsync(IntegrationTest test, string monitoredCollectionName)
        {
            Debug.Assert(test != null);
            Debug.Assert(monitoredCollectionName != null);

            DocumentCollectionInfo leaseCollectionInfo;
            IntegrationTestsHelper.GetConfigurationSettings(
                out test.ClassData.monitoredCollectionInfo,
                out leaseCollectionInfo,
                out monitoredOfferThroughput,
                out leaseOfferThroughput);

            test.ClassData.monitoredCollectionInfo.CollectionName = monitoredCollectionName;

            var monitoredCollection = new DocumentCollection
            {
                Id = test.ClassData.monitoredCollectionInfo.CollectionName,
            };

            if (test.ClassData.isPartitionedMonitoredCollection)
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

            using (var client = new DocumentClient(test.ClassData.monitoredCollectionInfo.Uri, test.ClassData.monitoredCollectionInfo.MasterKey, test.ClassData.monitoredCollectionInfo.ConnectionPolicy))
            {
                await IntegrationTestsHelper.CreateDocumentCollectionAsync(client, test.ClassData.monitoredCollectionInfo.DatabaseName, monitoredCollection, monitoredOfferThroughput);
            }

            test.FinishTestClassInitializeAsync().Wait();

            return leaseCollectionInfo;
        }

        private static async Task TestClassCleanupAsync(IntegrationTest test)
        {
            Debug.Assert(test != null);

            using (var client = new DocumentClient(test.ClassData.monitoredCollectionInfo.Uri, test.ClassData.monitoredCollectionInfo.MasterKey, test.ClassData.monitoredCollectionInfo.ConnectionPolicy))
            {
                await client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(
                    test.ClassData.monitoredCollectionInfo.DatabaseName, test.ClassData.monitoredCollectionInfo.CollectionName));
            }
        }

        private static int GetTestCount(Type testType)
        {
            Debug.Assert(testType != null);

            int testMethodCount = 0;
            foreach (var method in testType.GetMethods())
            {
                if (method.GetCustomAttribute(typeof(FactAttribute)) != null) testMethodCount++;
            }

            return testMethodCount;
        }
    }
}