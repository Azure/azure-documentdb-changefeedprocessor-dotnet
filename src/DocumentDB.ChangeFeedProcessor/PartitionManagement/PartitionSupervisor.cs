//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    internal class PartitionSupervisor : IPartitionSupervisor
    {
        private readonly ILease lease;
        private readonly IChangeFeedObserver observer;
        private readonly IPartitionProcessor processor;
        private readonly ILeaseRenewer renewer;
        private CancellationTokenSource processorCancellation;
        private readonly CancellationTokenSource renewerCancellation = new CancellationTokenSource();

        public PartitionSupervisor(ILease lease, IChangeFeedObserver observer, IPartitionProcessor processor, ILeaseRenewer renewer)
        {
            this.lease = lease;
            this.observer = observer;
            this.processor = processor;
            this.renewer = renewer;
        }

        public async Task RunAsync(CancellationToken shutdownToken)
        {
            var context = new ChangeFeedObserverContextInternal(lease.PartitionId);
            await observer.OpenAsync(context).ConfigureAwait(false);

            processorCancellation = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);
            Task processorTask = processor.RunAsync(processorCancellation.Token);
            processorTask.ContinueWith(_ => renewerCancellation.Cancel()).LogException();
            Task renewerTask = renewer.RunAsync(renewerCancellation.Token);
            renewerTask.ContinueWith(_ => processorCancellation.Cancel()).LogException();

            ChangeFeedObserverCloseReason closeReason = shutdownToken.IsCancellationRequested ? ChangeFeedObserverCloseReason.Shutdown : ChangeFeedObserverCloseReason.Unknown;

            try
            {
                await Task.WhenAll(processorTask, renewerTask).ConfigureAwait(false);
            }
            catch (LeaseLostException)
            {
                closeReason = ChangeFeedObserverCloseReason.LeaseLost;
                throw;
            }
            catch (PartitionSplitException)
            {
                closeReason = ChangeFeedObserverCloseReason.LeaseGone;
                throw;
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
                closeReason = ChangeFeedObserverCloseReason.Shutdown;
                // ignore
            }
            catch (Exception) when (processorTask.IsFaulted)
            {
                closeReason = ChangeFeedObserverCloseReason.ObserverError;
                throw;
            }
            finally
            {
                await observer.CloseAsync(context, closeReason).ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            processorCancellation?.Dispose();
            renewerCancellation.Dispose();
        }
    }
}