using System;
using System.Threading;

namespace KafkaClient.Common
{
    public class TimedCancellation : IDisposable
    {
        private readonly CancellationTokenSource _source;
        private readonly CancellationTokenRegistration _registration;

        public TimedCancellation(CancellationToken token, TimeSpan timeout)
        {
            _source = new CancellationTokenSource(timeout);
            _registration = token.Register(() => _source.Cancel());
        }

        public CancellationToken Token => _source.Token;

        public void Dispose()
        {
            using (_source) {
                using (_registration) {
                }
            }
        }
    }
}