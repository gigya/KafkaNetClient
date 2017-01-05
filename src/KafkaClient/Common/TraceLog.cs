using System;
using System.Diagnostics;

namespace KafkaClient.Common
{
    /// <summary>
    /// This class simply logs all information out to the Trace log provided by windows.
    /// The reason Trace is being used as the default it to remove extenal references from
    /// the base kafka-net package.  A proper logging framework like log4net is recommended.
    /// </summary>
    public class TraceLog : ILog
    {
        public static TraceLog Log { get; } = new TraceLog();

        private readonly LogLevel _minLevel;

        public TraceLog(LogLevel minLevel)
        {
            _minLevel = minLevel;
        }

        public TraceLog()
        {
            _minLevel = LogLevel.Debug;
        }

        /// <inheritdoc />
        public void Write(LogLevel level, Func<LogEvent> producer)
        {
            if (level >= _minLevel) {
                var logEvent = producer();
                var timestamp = DateTime.Now.ToString("hh:mm:ss-ffffff");
                var threadId = System.Threading.Thread.CurrentThread.ManagedThreadId;
                var message = $"{timestamp} [{level,-5}]";
                if (!string.IsNullOrEmpty(logEvent.Message)) {
                    message += $" Message=\"{logEvent.Message}\"";
                }
                if (logEvent.Exception != null) {
                    message += $"\r\nException=\"{logEvent.Exception}\"";
                }
                message += $" thread={threadId} in {logEvent.SourceFile}:{logEvent.SourceLine.GetValueOrDefault()}";
                Trace.WriteLine(message);
            }
        }
    }
}