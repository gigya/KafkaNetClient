using System;
using System.Threading;

namespace KafkaClient.Telemetry
{
    public class TcpStatistics : Statistics
    {
        public TcpStatistics(DateTimeOffset startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
        }

        private int _attempted = 0;
        public int Attempted => _attempted;

        private int _bytesAttempted = 0;
        public int BytesAttempted => _bytesAttempted;

        public void Attempt(int bytes = 0)
        {
            Interlocked.Increment(ref _attempted);
            Interlocked.Add(ref _bytesAttempted, bytes);
        }

        private int _started = 0;
        public int Started => _started;

        private int _bytesStarted = 0;
        public int BytesStarted => _bytesStarted;

        public void Start(int bytes)
        {
            Interlocked.Increment(ref _started);
            Interlocked.Add(ref _bytesStarted, bytes);
        }

        private int _successes = 0;
        public int Successes => _successes;

        private int _bytesSuccessful = 0;
        public int BytesSuccessful => _bytesSuccessful;

        public void Success(TimeSpan duration, int bytes)
        {
            Interlocked.Increment(ref _successes);
            Interlocked.Add(ref _duration, duration.Ticks);
            Interlocked.Add(ref _bytesSuccessful, bytes);
        }

        private long _duration = 0L;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        private int _failures = 0;
        public int Failures => _failures;

        public void Failure(TimeSpan duration)
        {
            Interlocked.Increment(ref _failures);
            Interlocked.Add(ref _duration, duration.Ticks);
        }
    }
}