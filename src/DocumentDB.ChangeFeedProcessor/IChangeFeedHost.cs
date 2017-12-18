using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    public interface IChangeFeedHost
    {
        Task StartAsync();
        Task StopAsync();
    }
}