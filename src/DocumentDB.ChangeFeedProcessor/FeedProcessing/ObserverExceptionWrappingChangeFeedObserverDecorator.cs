// ----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
// ----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;

    internal class ObserverExceptionWrappingChangeFeedObserverDecorator : IChangeFeedObserver
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private IChangeFeedObserver changeFeedObserver;

        public ObserverExceptionWrappingChangeFeedObserverDecorator(IChangeFeedObserver changeFeedObserver)
        {
            this.changeFeedObserver = changeFeedObserver;
        }

        public async Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            try
            {
                await this.changeFeedObserver.CloseAsync(context, reason).ConfigureAwait(false);
            }
            catch (Exception userException)
            {
                Logger.WarnException("Exception happened on Observer.CloseAsync", userException);
                throw new ObserverException(userException);
            }
        }

        public async Task OpenAsync(IChangeFeedObserverContext context)
        {
            try
            {
                await this.changeFeedObserver.OpenAsync(context).ConfigureAwait(false);
            }
            catch (Exception userException)
            {
                Logger.WarnException("Exception happened on Observer.OpenAsync", userException);
                throw new ObserverException(userException);
            }
        }

        public async Task ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            try
            {
                await this.changeFeedObserver.ProcessChangesAsync(context, docs, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception userException)
            {
                Logger.WarnException("Exception happened on Observer.ProcessChangesAsync", userException);
                throw new ObserverException(userException);
            }
        }
    }
}
