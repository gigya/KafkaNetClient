using System;
using System.Linq;
using System.Threading;

namespace KafkaClient.Common
{
    public class MergedCancellation : IDisposable
    {
        private readonly CancellationTokenSource _source;
        private readonly CancellationTokenRegistration[] _registrations;

        public MergedCancellation(params CancellationToken[] tokens)
        {
            _source = new CancellationTokenSource();
            _registrations = tokens.Select(t => t.Register(_source.Cancel)).ToArray();
            if (tokens.Any(t => t.IsCancellationRequested)) {
                _source.Cancel();
            }
        }

        public CancellationToken Token => _source.Token;

        public void Dispose()
        {
            using (_source) {
                foreach (var registration in _registrations) {
                    using (registration) {
                    }
                }
            }
        }
    }
}