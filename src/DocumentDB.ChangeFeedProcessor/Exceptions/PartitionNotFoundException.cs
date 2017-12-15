namespace DocumentDB.ChangeFeedProcessor.Exceptions
{
    public class PartitionNotFoundException : PartitionException
    {
        public PartitionNotFoundException(string lastContinuation) : base(lastContinuation)
        {
        }
    }
}