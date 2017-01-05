using System;

namespace KafkaClient.Common
{
    public class Retry : IRetry
    {
        private readonly int? _maxAttempts;

        public Retry(TimeSpan? timeout, int? maxAttempts = null)
        {
            _maxAttempts = maxAttempts;
            Timeout = timeout;
        }

        public TimeSpan? Timeout { get; }

        public TimeSpan? RetryDelay(int attempt, TimeSpan timeTaken) => ShouldRetry(attempt, timeTaken) ? GetDelay(attempt, timeTaken) : null;

        protected virtual TimeSpan? GetDelay(int attempt, TimeSpan timeTaken) => TimeSpan.Zero;

        public bool ShouldRetry(int attempt, TimeSpan timeTaken) => (!_maxAttempts.HasValue || attempt < _maxAttempts) && (!Timeout.HasValue || timeTaken < Timeout);
    }
}