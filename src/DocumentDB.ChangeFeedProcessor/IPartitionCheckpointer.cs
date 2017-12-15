using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor
{
    public interface IPartitionCheckpointer
    {
        /// <summary>
        /// Checkpoint the given partition up to the given continuation token. 
        /// </summary>
        /// <param name="сontinuationToken"></param>
        Task CheckpointPartitionAsync(string сontinuationToken);
    }
}