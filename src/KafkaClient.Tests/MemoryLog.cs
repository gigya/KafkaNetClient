using System;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Tests
{
    public class MemoryLog : ILog
    {
        public ImmutableList<Tuple<LogLevel, LogEvent>> LogEvents { get; private set; } = ImmutableList<Tuple<LogLevel, LogEvent>>.Empty;

        public bool WriteToConsole { get; set; } = true;

        /// <inheritdoc />
        public void Write(LogLevel level, Func<LogEvent> producer)
        {
            var logEvent = producer();
            LogEvents = LogEvents.Add(new Tuple<LogLevel, LogEvent>(level, logEvent));
            if (WriteToConsole) {
                ConsoleLog.Write(level, ConsoleLog.ToText(level, logEvent));
            }
        }
    }
}