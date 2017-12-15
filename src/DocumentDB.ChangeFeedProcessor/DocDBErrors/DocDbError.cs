namespace DocumentDB.ChangeFeedProcessor.DocDBErrors
{
    internal enum DocDbError
    {
        Undefined,
        PartitionNotFound,
        PartitionSplit,
        TransientError
    }
}