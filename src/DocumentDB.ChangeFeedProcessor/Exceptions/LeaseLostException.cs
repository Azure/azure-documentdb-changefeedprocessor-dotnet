using System;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions
{
#if NETFX
    [Serializable]
#endif
    public class LeaseLostException : Exception
    {
        /// <summary>Initializes a new instance of the <see cref="LeaseLostException" /> class using default values.</summary>
        public LeaseLostException()
        {
        }

        public LeaseLostException(ILease lease)
        {
            Lease = lease;
        }

        public LeaseLostException(ILease lease, Exception innerException, bool isGone = false)
            : base(null, innerException)
        {
            Lease = lease;
            IsGone = isGone;
        }

        public LeaseLostException(string message)
            : base(message)
        {
        }

        public LeaseLostException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

#if NETFX
        protected LeaseLostException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {
            this.Lease = (Lease)info.GetValue("Lease", typeof(Lease));
        }
#endif

        public ILease Lease { get; }

        public bool IsGone { get; }

#if NETFX
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            if (Lease != null)
            {
                info.AddValue("Lease", this.Lease);
            }
        }
#endif
    }
}