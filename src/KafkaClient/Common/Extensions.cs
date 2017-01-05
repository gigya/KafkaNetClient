using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace KafkaClient.Common
{
    public static class Extensions
    { 
        /// <summary>
        /// Splits a collection into given batch sizes and returns as an enumerable of batches.
        /// </summary>
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> collection, int batchSize)
        {
            var nextbatch = new List<T>(batchSize);
            foreach (T item in collection)
            {
                nextbatch.Add(item);
                if (nextbatch.Count == batchSize)
                {
                    yield return nextbatch;
                    nextbatch = new List<T>(batchSize);
                }
            }
            if (nextbatch.Count > 0)
                yield return nextbatch;
        }

        public static bool HasEqualElementsInOrder<T>(this IEnumerable<T> self, IEnumerable<T> other)
        {
            if (ReferenceEquals(self, other)) return true;
            if (ReferenceEquals(null, other)) return false;

            return self.Zip(other, (s, o) => Equals(s, o)).All(_ => _);
        }

        /// <summary>
        /// Attempts to prepare the exception for re-throwing by preserving the stack trace. The returned exception should be immediately thrown.
        /// </summary>
        /// <param name="exception">The exception. May not be <c>null</c>.</param>
        /// <returns>The <see cref="Exception"/> that was passed into this method.</returns>
        public static Exception PrepareForRethrow(this Exception exception)
        {
            ExceptionDispatchInfo.Capture(exception).Throw();

            // The code cannot ever get here. We just return a value to work around a badly-designed API (ExceptionDispatchInfo.Throw):
            //  https://connect.microsoft.com/VisualStudio/feedback/details/689516/exceptiondispatchinfo-api-modifications (http://www.webcitation.org/6XQ7RoJmO)
            return exception;
        }

        public static async Task<int> TryApplyAsync<T>(this AsyncProducerConsumerQueue<T> collection, Action<T> apply, CancellationToken cancellationToken)
        {
            var count = 0;
            try {
                while (true) {
                    var result = await collection.DequeueAsync(cancellationToken).ConfigureAwait(false);

                    apply(result);
                    count++;
                }
            } catch (InvalidOperationException) {
            } catch (OperationCanceledException) {
            }
            return count;
        }

        public static async Task EnqueueRangeAsync<T>(this AsyncProducerConsumerQueue<T> queue, IEnumerable<T> items, CancellationToken cancellationToken)
        {
            foreach (var item in items) {
                await queue.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }

        public static IEnumerable<T> Repeat<T>(this int count, Func<T> producer)
        {
            for (var i = 0; i < count; i++) {
                yield return producer();
            }
        }

        public static IEnumerable<T> Repeat<T>(this int count, Func<int, T> producer)
        {
            for (var i = 0; i < count; i++) {
                yield return producer(i);
            }
        }

        public static async Task AttemptAsync(
            this IRetry policy, 
            Func<int, Task> action, 
            Action<Exception, int, TimeSpan> onException, 
            Action<Exception, int> onFinalException, 
            CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var attempt = 0; !cancellationToken.IsCancellationRequested; attempt++) {
                cancellationToken.ThrowIfCancellationRequested();
                try {
                    await action(attempt).ConfigureAwait(false);
                    // reset attempt when successful
                    attempt = -1;
                    timer.Restart();
                } catch (Exception ex) {
                    if (attempt == 0) { // first failure
                        timer.Restart();
                    }
                    await policy.HandleErrorAndDelayAsync(onException, onFinalException, attempt, timer, ex, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public static async Task<T> AttemptAsync<T>(
            this IRetry policy, 
            Func<int, Stopwatch, Task<RetryAttempt<T>>> action, 
            Action<int, TimeSpan> onRetry, 
            Action<int> onFinal, 
            Action<Exception> onException, 
            CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var attempt = 0;; attempt++) {
                cancellationToken.ThrowIfCancellationRequested();
                try {
                    var response = await action(attempt, timer).ConfigureAwait(false);
                    if (response.IsSuccessful) return response.Value;

                    var retryDelay = policy.RetryDelay(attempt, timer.Elapsed);
                    if (retryDelay.HasValue) {
                        onRetry?.Invoke(attempt, retryDelay.Value);
                        await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
                    } else {
                        onFinal?.Invoke(attempt);
                        return response.Value;
                    }
                } catch (Exception ex) {
                    onException?.Invoke(ex);
                    onFinal?.Invoke(attempt);
                    return default(T);
                }
            }
        }

        public static async Task<T> AttemptAsync<T>(
            this IRetry policy, 
            Func<int, Stopwatch, Task<RetryAttempt<T>>> action, 
            Action<int, TimeSpan> onRetry, 
            Action<int> onFinal, 
            Action<Exception, int, TimeSpan> onException, 
            Action<Exception, int> onFinalException, 
            CancellationToken cancellationToken)
        {
            var timer = new Stopwatch();
            timer.Start();
            for (var attempt = 0;; attempt++) {
                cancellationToken.ThrowIfCancellationRequested();
                try {
                    var response = await action(attempt, timer).ConfigureAwait(false);
                    if (response.IsSuccessful) return response.Value;

                    var retryDelay = policy.RetryDelay(attempt, timer.Elapsed);
                    if (retryDelay.HasValue) {
                        onRetry?.Invoke(attempt, retryDelay.Value);
                        await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
                    } else {
                        onFinal?.Invoke(attempt);
                        return response.Value;
                    }
                } catch (Exception ex) {
                    await policy.HandleErrorAndDelayAsync(onException, onFinalException, attempt, timer, ex, cancellationToken).ConfigureAwait(false);;
                }
            }
        }

        private static async Task HandleErrorAndDelayAsync(
            this IRetry policy, 
            Action<Exception, int, TimeSpan> onException, 
            Action<Exception, int> onFinalException, 
            int attempt, 
            Stopwatch timer, 
            Exception ex, 
            CancellationToken cancellationToken)
        {
            var retryDelay = policy.RetryDelay(attempt, timer.Elapsed);
            if (retryDelay.HasValue) {
                onException?.Invoke(ex, attempt, retryDelay.Value);
                await Task.Delay(retryDelay.Value, cancellationToken).ConfigureAwait(false);
            } else {
                onFinalException?.Invoke(ex, attempt);
                throw ex.PrepareForRethrow();
            }
        }

        public static IImmutableList<T> AddNotNull<T>(this IImmutableList<T> list, T item) where T : class
        {
            return item != null ? list.Add(item) : list;
        }

        public static IImmutableList<T> AddNotNullRange<T>(this IImmutableList<T> list, IEnumerable<T> items)
        {
            return items != null ? list.AddRange(items) : list;
        }
        public static IImmutableDictionary<T, TValue> AddNotNullRange<T, TValue>(this IImmutableDictionary<T, TValue> dictionary, IEnumerable<KeyValuePair<T, TValue>> items)
        {
            return items != null ? dictionary.AddRange(items) : dictionary;
        }

        public static IEnumerable<byte> ToEnumerable(this Stream stream)
        {
            if (stream == null) yield break;

            int value;
            while (-1 != (value = stream.ReadByte())) yield return (byte)value;
        }

        public static Exception FlattenAggregates(this IEnumerable<Exception> exceptions)
        {
            var exceptionList = exceptions.ToArray();
            if (exceptionList.Length == 1) return exceptionList[0];

            return new AggregateException(exceptionList.SelectMany<Exception, Exception>(
                ex => {
                    var aggregateException = ex as AggregateException;
                    if (aggregateException != null) return aggregateException.InnerExceptions;
                    return new[] { ex };
                }));
            
        }
    }
}