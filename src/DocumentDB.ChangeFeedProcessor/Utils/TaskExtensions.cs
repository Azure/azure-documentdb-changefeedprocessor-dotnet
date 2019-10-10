//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------
namespace Microsoft.Azure.Documents.ChangeFeedProcessor.Utils
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;

    internal static class TaskExtensions
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        public static void LogException(this Task task)
        {
            task.ContinueWith(_ => Logger.ErrorException("exception caught", task.Exception), TaskContinuationOptions.OnlyOnFaulted);
        }

        public static async Task IgnoreException(this Task task)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // ignore
            }
        }

        public static async Task<TResult> AbortOnTimeout<TResult>(this Task<TResult> responseTask, TimeSpan timeout, CancellationToken token)
        {
            using (CancellationTokenSource cancelSource = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                Task timeoutTask = Task.Delay(timeout, cancelSource.Token);

                if (responseTask == await Task.WhenAny(responseTask, timeoutTask).ConfigureAwait(false))
                {
                    cancelSource.Cancel(); // cancel the delay task
                    return responseTask.GetAwaiter().GetResult(); // propagate exception
                }
            }

            // timeout or cancellation token has fired
            token.ThrowIfCancellationRequested();
            throw new TimeoutException();
        }
    }
}