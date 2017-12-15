namespace DocumentDB.ChangeFeedProcessor.Exceptions
{
    public class PartitionSplitException : PartitionException
    {
        public PartitionSplitException(string lastContinuation) : base(lastContinuation)
        {
        }
    }
}