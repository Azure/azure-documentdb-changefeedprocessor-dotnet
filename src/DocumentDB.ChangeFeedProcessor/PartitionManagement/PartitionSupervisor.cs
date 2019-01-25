//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Utils;

    internal class PartitionSupervisor : IPartitionSupervisor
    {
        private readonly ILease lease;
        private readonly IChangeFeedObserver observer;
        private readonly IPartitionProcessor processor;
        private readonly ILeaseRenewer renewer;
        private readonly CancellationTokenSource renewerCancellation = new CancellationTokenSource();
        private CancellationTokenSource processorCancellation;

        public PartitionSupervisor(ILease lease, IChangeFeedObserver observer, IPartitionProcessor processor, ILeaseRenewer renewer)
        {
            this.lease = lease;
            this.observer = observer;
            this.processor = processor;
            this.renewer = renewer;
        }

        public async Task RunAsync(CancellationToken shutdownToken)
        {
            var context = new ChangeFeedObserverContext(this.lease.PartitionId);
            await this.observer.OpenAsync(context).ConfigureAwait(false);

            this.processorCancellation = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);

            Task processorTask = this.processor.RunAsync(this.processorCancellation.Token);
            processorTask.ContinueWith(_ => this.renewerCancellation.Cancel()).LogException();

            Task renewerTask = this.renewer.RunAsync(this.renewerCancellation.Token);
            renewerTask.ContinueWith(_ => this.processorCancellation.Cancel()).LogException();

            ChangeFeedObserverCloseReason closeReason = shutdownToken.IsCancellationRequested ?
                ChangeFeedObserverCloseReason.Shutdown :
                ChangeFeedObserverCloseReason.Unknown;

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
            }
            catch (ObserverException)
            {
                closeReason = ChangeFeedObserverCloseReason.ObserverError;
                throw;
            }
            catch (Exception) when (processorTask.IsFaulted)
            {
                closeReason = ChangeFeedObserverCloseReason.Unknown;
                throw;
            }
            finally
            {
                await this.observer.CloseAsync(context, closeReason).ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            this.processorCancellation?.Dispose();
            this.renewerCancellation.Dispose();
        }
    }
}