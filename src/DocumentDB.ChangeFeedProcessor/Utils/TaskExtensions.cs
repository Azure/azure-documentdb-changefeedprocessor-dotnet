using System;
using System.Threading.Tasks;
using DocumentDB.ChangeFeedProcessor.Logging;

namespace DocumentDB.ChangeFeedProcessor.Utils
{
    internal static class TaskExtensions
    {
        private static readonly ILog logger = LogProvider.GetCurrentClassLogger();

        public static void LogException(this Task task)
        {
            task.ContinueWith(_ => logger.ErrorException("exception caught", task.Exception), TaskContinuationOptions.OnlyOnFaulted);
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
    }
}