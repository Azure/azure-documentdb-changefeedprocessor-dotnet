using System;

namespace DocumentDB.ChangeFeedProcessor.Exceptions
{
    public class PartitionException : Exception
    {
        public string LastContinuation { get; }

        public PartitionException(string lastContinuation)
        {
            LastContinuation = lastContinuation;
        }
    }
}