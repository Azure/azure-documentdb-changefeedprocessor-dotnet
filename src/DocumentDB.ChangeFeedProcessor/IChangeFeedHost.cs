using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor
{
    public interface IChangeFeedHost
    {
        Task StartAsync();
        Task StopAsync();
    }
}