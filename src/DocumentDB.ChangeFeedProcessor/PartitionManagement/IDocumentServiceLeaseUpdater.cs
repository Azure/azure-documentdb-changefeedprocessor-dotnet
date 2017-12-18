using System;
using System.Threading.Tasks;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    public interface IDocumentServiceLeaseUpdater
    {
        Task<ILease> UpdateLeaseAsync(ILease cachedLease, Uri documentUri, Func<ILease, ILease> updateLease);
    }
}