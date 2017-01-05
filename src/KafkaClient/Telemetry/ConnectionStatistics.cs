using System;
using System.Threading;

namespace KafkaClient.Telemetry
{
    public class ConnectionStatistics : Statistics
    {
        public ConnectionStatistics(DateTimeOffset startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
        }

        private int _attempted = 0;
        public int Attempted => _attempted;

        public void Attempt()
        {
            Interlocked.Increment(ref _attempted);
        }

        private int _successes = 0;
        public int Successes => _successes;

        private long _duration = 0L;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        public void Success(TimeSpan duration)
        {
            Interlocked.Increment(ref _successes);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        private int _failures = 0;
        public int Failures => _failures;

        public void Failure()
        {
            Interlocked.Increment(ref _failures);
        }
    }
}