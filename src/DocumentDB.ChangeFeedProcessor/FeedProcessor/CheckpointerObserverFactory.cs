//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    using System;

    /// <summary>
    /// Factory class used to create instance(s) of <see cref="IChangeFeedObserver"/>.
    /// </summary>
#if PRIVATE_API
    public
#else
    internal
#endif
    class CheckpointerObserverFactory : IObserverFactory
    {
        private readonly IObserverFactory observerFactory;
        private readonly CheckpointFrequency checkpointFrequency;

        /// <summary>
        /// Initializes a new instance of the <see cref="CheckpointerObserverFactory"/> class.
        /// </summary>
        /// <param name="observerFactory">Instance of Observer Factory</param>
        /// <param name="checkpointFrequency">Defined <see cref="CheckpointFrequency"/></param>
        public CheckpointerObserverFactory(IObserverFactory observerFactory, CheckpointFrequency checkpointFrequency)
        {
            if (observerFactory == null)
                throw new ArgumentNullException(nameof(observerFactory));
            if (checkpointFrequency == null)
                throw new ArgumentNullException(nameof(checkpointFrequency));

            this.observerFactory = observerFactory;
            this.checkpointFrequency = checkpointFrequency;
        }

        /// <summary>
        /// Creates a new instance of <see cref="IChangeFeedObserver"/>.
        /// </summary>
        /// <returns>Created instance of <see cref="IChangeFeedObserver"/>.</returns>
        public IObserver CreateObserver()
        {
            IObserver observer = this.observerFactory.CreateObserver();
            if (this.checkpointFrequency.ExplicitCheckpoint) return observer;

            return new AutoCheckpointer(this.checkpointFrequency, observer);
        }
    }
}