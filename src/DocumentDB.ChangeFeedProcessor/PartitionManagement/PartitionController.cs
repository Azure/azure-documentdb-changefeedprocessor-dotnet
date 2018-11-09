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
    using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

    internal class PartitionController : IPartitionController
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> currentlyOwnedPartitions = new ConcurrentDictionary<string, TaskCompletionSource<bool>>();

        private readonly ILeaseContainer leaseContainer;
        private readonly ILeaseManager leaseManager;
        private readonly IPartitionSupervisorFactory partitionSupervisorFactory;
        private readonly IPartitionSynchronizer synchronizer;
        private readonly CancellationTokenSource shutdownCts = new CancellationTokenSource();

        public PartitionController(
            ILeaseContainer leaseContainer,
            ILeaseManager leaseManager,
            IPartitionSupervisorFactory partitionSupervisorFactory,
            IPartitionSynchronizer synchronizer)
        {
            this.leaseContainer = leaseContainer;
            this.leaseManager = leaseManager;
            this.partitionSupervisorFactory = partitionSupervisorFactory;
            this.synchronizer = synchronizer;
        }

        public async Task InitializeAsync()
        {
            await this.LoadLeasesAsync().ConfigureAwait(false);
        }

        public async Task AddOrUpdateLeaseAsync(ILease lease)
        {
            var tcs = new TaskCompletionSource<bool>();

            if (!this.currentlyOwnedPartitions.TryAdd(lease.PartitionId, tcs))
            {
                await this.leaseManager.UpdatePropertiesAsync(lease).ConfigureAwait(false);
                Logger.DebugFormat("partition {0}: updated", lease.PartitionId);
                return;
            }

            try
            {
                var updatedLease = await this.leaseManager.AcquireAsync(lease).ConfigureAwait(false);
                if (updatedLease != null) lease = updatedLease;
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
            foreach (ILease lease in await this.leaseContainer.GetOwnedLeasesAsync().ConfigureAwait(false))
            {
                Logger.InfoFormat("Acquired lease for PartitionId '{0}' on startup.", lease.PartitionId);
                addLeaseTasks.Add(this.AddOrUpdateLeaseAsync(lease));
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
            catch (PartitionSplitException ex)
            {
                await this.HandleSplitAsync(lease, ex.LastContinuation).ConfigureAwait(false);
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

        private async Task HandleSplitAsync(ILease lease, string lastContinuationToken)
        {
            try
            {
                lease.ContinuationToken = lastContinuationToken;
                IEnumerable<ILease> addedLeases = await this.synchronizer.SplitPartitionAsync(lease).ConfigureAwait(false);
                Task[] addLeaseTasks = addedLeases.Select(l =>
                    {
                        l.Properties = lease.Properties;
                        return this.AddOrUpdateLeaseAsync(l);
                    }).ToArray();

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