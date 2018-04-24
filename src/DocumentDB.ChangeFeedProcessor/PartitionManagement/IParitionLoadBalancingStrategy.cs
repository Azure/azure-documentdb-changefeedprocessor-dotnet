//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement
{
    using System.Collections.Generic;

    /// <summary>
    /// A strategy defines which leases should be taken by the current host in a certain moment.
    /// </summary>
    /// <remarks>
    /// It can set new <see cref="ILease.Properties"/> for all returned leases if needed, including currently owned leases.
    /// </remarks>
    /// <example>
    /// <code language="C#">
    /// <![CDATA[
    /// public class CustomStrategy : IParitionLoadBalancingStrategy
    /// {
    ///     private string hostName;
    ///     private string hostVersion;
    ///     private TimeSpan leaseExpirationInterval;
    ///
    ///     private const string VersionPropertyKey = "version";
    ///
    ///     public IEnumerable<ILease> SelectLeasesToTake(IEnumerable<ILease> allLeases)
    ///     {
    ///         var takenLeases = this.FindLeasesToTake(allLeases);
    ///         foreach (var lease in takenLeases)
    ///         {
    ///             lease.Properties[VersionPropertyKey] = this.hostVersion;
    ///         }
    ///
    ///         return takenLeases;
    ///     }
    ///
    ///     private IEnumerable<ILease> FindLeasesToTake(IEnumerable<ILease> allLeases)
    ///     {
    ///         List<ILease> takenLeases = new List<ILease>();
    ///         foreach (var lease in allLeases)
    ///         {
    ///             if (string.IsNullOrWhiteSpace(lease.Owner) || this.IsExpired(lease))
    ///             {
    ///                 takenLeases.Add(lease);
    ///             }
    ///
    ///             if (lease.Owner != this.hostName)
    ///             {
    ///                 var ownerVersion = lease.Properties[VersionPropertyKey];
    ///                 if (ownerVersion < this.hostVersion)
    ///                 {
    ///                     takenLeases.Add(lease);
    ///                 }
    ///
    ///                 // more logic for leases owned by other hosts
    ///             }
    ///         }
    ///
    ///         return takenLeases;
    ///     }
    ///
    ///     private bool IsExpired(ILease lease)
    ///     {
    ///         return lease.Timestamp.ToUniversalTime() + this.leaseExpirationInterval < DateTime.UtcNow;
    ///     }
    /// }
    /// ]]>
    /// </code>
    /// </example>
    public interface IParitionLoadBalancingStrategy
    {
        /// <summary>
        /// Select leases that should be taken for processing.
        /// This method will be called periodically with <see cref="ChangeFeedProcessorOptions.LeaseAcquireInterval"/>
        /// </summary>
        /// <param name="allLeases">All leases</param>
        /// <returns>Leases that should be taken for processing by this host</returns>
        IEnumerable<ILease> SelectLeasesToTake(IEnumerable<ILease> allLeases);
    }
}