//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping
{
    using System.Threading.Tasks;

    public interface IBootstrapper
    {
        Task InitializeAsync();
    }
}