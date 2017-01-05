using System;
using System.Runtime.CompilerServices;

namespace KafkaClient.Common
{
    public struct LogEvent
    {
        public static LogEvent Create(
            string message, 
            [CallerFilePath] string sourceFile = null, 
            [CallerLineNumber] int sourceLine = 0)
        {
            return new LogEvent(message, null, sourceFile, sourceLine > 0 ? sourceLine : (int?)null);
        }

        public static LogEvent Create(
            Exception exception,
            string message = null, 
            [CallerFilePath] string sourceFile = null, 
            [CallerLineNumber] int sourceLine = 0)
        {
            return new LogEvent(message, exception, sourceFile, sourceLine > 0 ? sourceLine : (int?)null);
        }

        public LogEvent(string message, Exception exception, string sourceFile, int? sourceLine)
        {
            Message = message;
            Exception = exception;
            SourceFile = sourceFile;
            SourceLine = sourceLine;
        }

        public string Message { get; }
        public Exception Exception { get; }
        public string SourceFile { get; }
        public int? SourceLine { get; }
    }
}