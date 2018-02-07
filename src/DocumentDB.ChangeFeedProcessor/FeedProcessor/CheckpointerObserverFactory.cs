﻿//----------------------------------------------------------------
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
        private readonly CheckpointFrequency checkpointFrequency;

        /// <summary>
        /// Factory class used to create instance(s) of <see cref="IChangeFeedObserver"/>
        /// </summary>
        /// <param name="observerFactory">Instance of Observer Factory</param>
        /// <param name="checkpointFrequency">Defined checkpoint frequency</param>
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
        /// Creates a new instance of <see cref="IChangeFeedObserver"/>.
        /// </summary>
        /// <returns>Created instance of <see cref="IChangeFeedObserver"/>.</returns>
        public IChangeFeedObserver CreateObserver()
        {
            IChangeFeedObserver observer = this.observerFactory.CreateObserver();
            if (this.checkpointFrequency.ExplicitCheckpoint) return observer;

            return new AutoCheckpointer(this.checkpointFrequency, observer);
        }
    }
}