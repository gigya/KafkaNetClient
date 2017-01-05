using System;

namespace KafkaClient.Common
{
    public interface ILog
    {
        /// <summary>
        /// Record logging details lazily to avoid unnecessary computation costs
        /// </summary>
        void Write(LogLevel level, Func<LogEvent> producer);
    }
}