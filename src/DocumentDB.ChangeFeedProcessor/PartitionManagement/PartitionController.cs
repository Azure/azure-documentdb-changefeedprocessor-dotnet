//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

    internal class PartitionController : IPartitionController
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private readonly string hostName;

        private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> currentlyOwnedPartitions = new ConcurrentDictionary<string, TaskCompletionSource<bool>>();

        private readonly ILeaseManager leaseManager;
        private readonly IPartitionSupervisorFactory partitionSupervisorFactory;
        private readonly IPartitionSynchronizer synchronizer;
        private readonly CancellationTokenSource shutdownCts = new CancellationTokenSource();

        public PartitionController(string hostName, ILeaseManager leaseManager, IPartitionSupervisorFactory partitionSupervisorFactory, IPartitionSynchronizer synchronizer)
        {
            this.hostName = hostName;
            this.leaseManager = leaseManager;
            this.partitionSupervisorFactory = partitionSupervisorFactory;
            this.synchronizer = synchronizer;
        }

        public async Task InitializeAsync()
        {
            await this.LoadLeasesAsync().ConfigureAwait(false);
        }

        public async Task AddLeaseAsync(ILease lease)
        {
            var tcs = new TaskCompletionSource<bool>();
            if (!this.currentlyOwnedPartitions.TryAdd(lease.PartitionId, tcs))
            {
                return;
            }

            try
            {
                await this.leaseManager.AcquireAsync(lease, this.hostName).ConfigureAwait(false);
                Logger.InfoFormat("partition {0}: acquired", lease.PartitionId);
            }
            catch (Exception)
            {
                await this.RemoveLeaseAsync(lease).ConfigureAwait(false);
                throw;
            }

            IPartitionSupervisor supervisor = this.partitionSupervisorFactory.Create(lease);
            this.ProcessPartition(supervisor, lease).LogException();
        }

        public async Task ShutdownAsync()
        {
            this.shutdownCts.Cancel();
            IEnumerable<Task> leases = this.currentlyOwnedPartitions.Select(pair => pair.Value.Task).ToList();
            await Task.WhenAll(leases).ConfigureAwait(false);
        }

        private async Task LoadLeasesAsync()
        {
            Logger.Debug("Starting renew leases assigned to this host on initialize.");
            var addLeaseTasks = new List<Task>();
            foreach (ILease lease in await this.leaseManager.ListLeasesAsync().ConfigureAwait(false))
            {
                if (string.Compare(lease.Owner, this.hostName, StringComparison.OrdinalIgnoreCase) == 0)
                {
                    Logger.InfoFormat("Acquired lease for PartitionId '{0}' on startup.", lease.PartitionId);
                    addLeaseTasks.Add(this.AddLeaseAsync(lease));
                }
            }

            await Task.WhenAll(addLeaseTasks.ToArray()).ConfigureAwait(false);
        }

        private async Task RemoveLeaseAsync(ILease lease)
        {
            TaskCompletionSource<bool> worker;
            if (!this.currentlyOwnedPartitions.TryRemove(lease.PartitionId, out worker))
            {
                return;
            }

            Logger.InfoFormat("partition {0}: released", lease.PartitionId);

            try
            {
                await this.leaseManager.ReleaseAsync(lease).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.WarnException("partition {0}: failed to remove lease", e, lease.PartitionId);
            }
            finally
            {
                worker.SetResult(false);
            }
        }

        private async Task ProcessPartition(IPartitionSupervisor partitionSupervisor, ILease lease)
        {
            try
            {
                await partitionSupervisor.RunAsync(this.shutdownCts.Token).ConfigureAwait(false);
            }
            catch (PartitionSplitException)
            {
                await this.HandleSplitAsync(lease).ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                Logger.DebugFormat("partition {0}: processing canceled", lease.PartitionId);
            }
            catch (Exception e)
            {
                Logger.WarnException("partition {0}: processing failed", e, lease.PartitionId);
            }

            await this.RemoveLeaseAsync(lease).ConfigureAwait(false);
        }

        private async Task HandleSplitAsync(ILease lease)
        {
            try
            {
                IEnumerable<ILease> addedLeases = await this.synchronizer.SplitPartitionAsync(lease).ConfigureAwait(false);
                Task[] addLeaseTasks = addedLeases.Select(this.AddLeaseAsync).ToArray();
                await this.leaseManager.DeleteAsync(lease).ConfigureAwait(false);
                await Task.WhenAll(addLeaseTasks).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.WarnException("partition {0}: failed to split", e, lease.PartitionId);
            }
        }
    }
}