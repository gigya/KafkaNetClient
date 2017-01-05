using System;

namespace KafkaClient.Common
{
    public static class LogExtensions
    {
        /// <summary>
        /// Record debug information to the log.
        /// </summary>
        public static void Debug(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Debug, producer);
        }

        /// <summary>
        /// Record information to the log.
        /// </summary>
        public static void Info(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Info, producer);
        }

        /// <summary>
        /// Record warning information to the log.
        /// </summary>
        public static void Warn(this ILog log, Func<LogEvent> producer)
        {
            log.Write(LogLevel.Warn, producer);
        }

        /// <summary>
        /// Record error information to the log.
        /// </summary>
        public static void Error(this ILog log, LogEvent logEvent)
        {
            log.Write(LogLevel.Error, () => logEvent);
        }
    }
}