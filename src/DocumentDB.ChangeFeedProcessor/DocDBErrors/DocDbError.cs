namespace Microsoft.Azure.Documents.ChangeFeedProcessor.DocDBErrors
{
    internal enum DocDbError
    {
        Undefined,
        PartitionNotFound,
        PartitionSplit,
        TransientError
    }
}