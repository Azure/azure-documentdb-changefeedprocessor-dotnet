//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

namespace Microsoft.Azure.Documents.ChangeFeedProcessor
{
    using System;
    using System.Threading.Tasks;

    internal interface IUnhealthyHandlingStrategy
    {
        Task HandleAsync();
    }

    internal class FailFastUnhealthyHandlingStrategy : IUnhealthyHandlingStrategy
    {
        public Task HandleAsync()
        {
            Environment.FailFast("Unhealthy instance detected.");

            return Task.CompletedTask;
        }
    }
}