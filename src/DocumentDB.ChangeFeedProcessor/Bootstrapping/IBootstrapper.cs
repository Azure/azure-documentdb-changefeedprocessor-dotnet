using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping
{
    public interface IBootstrapper
    {
        Task InitializeAsync();
    }
}