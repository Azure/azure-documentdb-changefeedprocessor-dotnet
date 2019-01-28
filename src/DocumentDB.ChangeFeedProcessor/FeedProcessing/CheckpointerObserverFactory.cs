//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
{
    using System;

    /// <summary>
    /// Factory class used to create instance(s) of <see cref="Documents.ChangeFeedProcessor.IChangeFeedObserver"/>.
    /// </summary>
    internal class CheckpointerObserverFactory : IChangeFeedObserverFactory
    {
        private readonly IChangeFeedObserverFactory observerFactory;
        private readonly CheckpointFrequency checkpointFrequency;

        /// <summary>
        /// Initializes a new instance of the <see cref="CheckpointerObserverFactory"/> class.
        /// </summary>
        /// <param name="observerFactory">Instance of Observer Factory</param>
        /// <param name="checkpointFrequency">Defined <see cref="CheckpointFrequency"/></param>
        public CheckpointerObserverFactory(IChangeFeedObserverFactory observerFactory, CheckpointFrequency checkpointFrequency)
        {
            if (observerFactory == null)
                throw new ArgumentNullException(nameof(observerFactory));
            if (checkpointFrequency == null)
                throw new ArgumentNullException(nameof(checkpointFrequency));

            this.observerFactory = observerFactory;
            this.checkpointFrequency = checkpointFrequency;
        }

        /// <summary>
        /// Creates a new instance of <see cref="Documents.ChangeFeedProcessor.IChangeFeedObserver"/>.
        /// </summary>
        /// <returns>Created instance of <see cref="Documents.ChangeFeedProcessor.IChangeFeedObserver"/>.</returns>
        public IChangeFeedObserver CreateObserver()
        {
            IChangeFeedObserver observer = new ObserverExceptionWrappingChangeFeedObserverDecorator(this.observerFactory.CreateObserver());
            if (this.checkpointFrequency.ExplicitCheckpoint) return observer;

            return new AutoCheckpointer(this.checkpointFrequency, observer);
        }
    }
}