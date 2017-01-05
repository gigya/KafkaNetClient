using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Common
{
    internal class CancellableTask<T> : IDisposable
    {
        public CancellableTask(CancellationToken cancellationToken)
        {
            Tcs = new TaskCompletionSource<T>();
            CancellationToken = cancellationToken;
            _cancellationTokenRegistration = cancellationToken.Register(() => Tcs.TrySetCanceled());
        }

        public CancellationToken CancellationToken { get; }
        public TaskCompletionSource<T> Tcs { get; }

        private CancellationTokenRegistration _cancellationTokenRegistration;

        public void Dispose()
        {
            _cancellationTokenRegistration.Dispose();
        }
    }
}