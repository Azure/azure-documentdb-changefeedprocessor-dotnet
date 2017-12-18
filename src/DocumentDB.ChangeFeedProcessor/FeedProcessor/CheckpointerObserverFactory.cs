namespace Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessor
{
    class CheckpointerObserverFactory : IChangeFeedObserverFactory
    {
        private readonly IChangeFeedObserverFactory observerFactory;
        private readonly CheckpointFrequency checkpointFrequency;

        public CheckpointerObserverFactory(IChangeFeedObserverFactory observerFactory, CheckpointFrequency checkpointFrequency)
        {
            this.observerFactory = observerFactory;
            this.checkpointFrequency = checkpointFrequency;
        }

        public IChangeFeedObserver CreateObserver()
        {
            IChangeFeedObserver observer = observerFactory.CreateObserver();
            if (checkpointFrequency.ExplicitCheckpoint) return observer;

            return new AutoCheckpointer(checkpointFrequency, observer);
        }
    }
}