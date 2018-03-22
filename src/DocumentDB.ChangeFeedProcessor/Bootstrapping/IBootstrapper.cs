//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Bootstrapping
{
    using System.Threading.Tasks;

    internal interface IBootstrapper
    {
        Task InitializeAsync();
    }
}