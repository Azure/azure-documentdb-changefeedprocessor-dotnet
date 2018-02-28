//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    using System;

    /// <summary>
    /// Factory class used to create instance(s) of <see cref="IChangeFeedObserver"/>.
    /// </summary>
    public class CheckpointerObserverFactory : IChangeFeedObserverFactory
    {
        private readonly IChangeFeedObserverFactory observerFactory;
        private readonly ChangeFeedHostOptions changeFeedHostOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="CheckpointerObserverFactory"/> class.
        /// </summary>
        /// <param name="observerFactory">Instance of Observer Factory</param>
        /// <param name="changeFeedHostOptions">Defined <see cref="ChangeFeedHostOptions"/></param>
        public CheckpointerObserverFactory(IChangeFeedObserverFactory observerFactory, ChangeFeedHostOptions changeFeedHostOptions)
        {
            if (observerFactory == null)
                throw new ArgumentNullException(nameof(observerFactory));
            if (changeFeedHostOptions == null)
                throw new ArgumentNullException(nameof(changeFeedHostOptions));

            this.observerFactory = observerFactory;
            this.changeFeedHostOptions = changeFeedHostOptions;
        }

        /// <summary>
        /// Creates a new instance of <see cref="IChangeFeedObserver"/>.
        /// </summary>
        /// <returns>Created instance of <see cref="IChangeFeedObserver"/>.</returns>
        public IChangeFeedObserver CreateObserver()
        {
            IChangeFeedObserver observer = this.observerFactory.CreateObserver();
            if (this.changeFeedHostOptions.CheckpointFrequency.ExplicitCheckpoint || !this.changeFeedHostOptions.IsAutoCheckpointEnabled) return observer;

            return new AutoCheckpointer(this.changeFeedHostOptions.CheckpointFrequency, observer);
        }
    }
}