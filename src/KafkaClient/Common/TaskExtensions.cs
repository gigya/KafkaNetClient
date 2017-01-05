using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    /// <summary>
    /// Utility functions for dealing with Task's.
    /// </summary>
    /// <remarks>
    /// Some of these come from orleans TaskExtensions here: https://github.com/dotnet/orleans/blob/master/src/Orleans/Async/TaskExtensions.cs#L218
    /// Others come from Stephen Cleary here: https://github.com/StephenCleary/AsyncEx.Tasks/blob/master/src/Nito.AsyncEx.Tasks/SynchronousTaskExtensions.cs
    /// </remarks>
    public static class TaskExtensions
    {
        /// <summary>
        /// Execute an await task while monitoring a given cancellation token.  Use with non-cancelable async operations.
        /// </summary>
        /// <remarks>
        /// This extension method will only cancel the await and not the actual IO operation.  The status of the IO opperation will still
        /// need to be considered after the operation is cancelled.
        /// See <see cref="http://blogs.msdn.com/b/pfxteam/archive/2012/10/05/how-do-i-cancel-non-cancelable-async-operations.aspx"/>
        /// </remarks>
        public static async Task<T> ThrowIfCancellationRequested<T>(this Task<T> task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(_ => ((TaskCompletionSource<bool>)_).TrySetResult(true), tcs)) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
            return await task.ConfigureAwait(false);
        }

        /// <summary>
        /// Execute an await task while monitoring a given cancellation token.  Use with non-cancelable async operations.
        /// </summary>
        /// <remarks>
        /// This extension method will only cancel the await and not the actual IO operation.  The status of the IO opperation will still
        /// need to be considered after the operation is cancelled.
        /// See <see cref="http://blogs.msdn.com/b/pfxteam/archive/2012/10/05/how-do-i-cancel-non-cancelable-async-operations.aspx"/>
        /// </remarks>
        public static async Task ThrowIfCancellationRequested(this Task task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(_ => ((TaskCompletionSource<bool>)_).TrySetResult(true), tcs)) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    throw new OperationCanceledException(cancellationToken);
                }
            }
        }

        public static async Task<bool> IsCancelled(this Task task, CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            using (cancellationToken.Register(() => tcs.TrySetResult(true))) {
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false)) {
                    return true;
                }
            }
            return false;
        }
    }
}
