namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
    public class PartitionNotFoundException : PartitionException
    {
        public PartitionNotFoundException(string lastContinuation) : base(lastContinuation)
        {
        }
    }
}