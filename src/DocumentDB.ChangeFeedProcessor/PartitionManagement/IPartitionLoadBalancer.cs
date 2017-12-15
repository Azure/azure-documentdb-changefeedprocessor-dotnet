using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.PartitionManagement
{
    public interface IPartitionLoadBalancer
    {
        /// <summary>
        /// Starts the load balancer
        /// </summary>
        void Start();

        /// <summary>
        /// Stops the load balancer
        /// </summary>
        /// <returns>Task that completes once load balancer is fully stopped</returns>
        Task StopAsync();
    }
}