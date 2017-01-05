using System;

namespace KafkaClient.Telemetry
{
    public abstract class Statistics
    {
        protected Statistics(DateTimeOffset startedAt, TimeSpan duration)
        {
            StartedAt = startedAt;
            EndedAt = startedAt.Add(duration);
        }

        public DateTimeOffset StartedAt { get; }
        public DateTimeOffset EndedAt { get; }
    }
}