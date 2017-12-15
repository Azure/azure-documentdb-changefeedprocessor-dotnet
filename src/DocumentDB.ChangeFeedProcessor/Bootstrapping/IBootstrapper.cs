using System.Threading.Tasks;

namespace DocumentDB.ChangeFeedProcessor.Bootstrapping
{
    public interface IBootstrapper
    {
        Task InitializeAsync();
    }
}