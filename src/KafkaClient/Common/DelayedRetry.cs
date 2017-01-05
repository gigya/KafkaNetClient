using System;

namespace KafkaClient.Common
{
    public class DelayedRetry : Retry
    {
        private readonly TimeSpan _delay;

        public DelayedRetry(TimeSpan timeout, TimeSpan delay, int? maxAttempts = null)
            : base (timeout, maxAttempts)
        {
            _delay = delay;
        }

        protected override TimeSpan? GetDelay(int attempt, TimeSpan timeTaken) => _delay > Timeout - timeTaken ? (TimeSpan?)null : _delay;
    }
}