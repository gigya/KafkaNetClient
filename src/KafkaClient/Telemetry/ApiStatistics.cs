using System;
using System.Collections.Concurrent;
using System.Threading;
using KafkaClient.Protocol;

namespace KafkaClient.Telemetry
{
    public class ApiStatistics : Statistics
    {
        public ApiStatistics(DateTimeOffset startedAt, TimeSpan duration)
            : base(startedAt, duration)
        {
            RequestsAttempted = new ConcurrentDictionary<ApiKeyRequestType, int>();
            Requests = new ConcurrentDictionary<ApiKeyRequestType, int>();
            RequestsFailed = new ConcurrentDictionary<ApiKeyRequestType, int>();
        }

        public ConcurrentDictionary<ApiKeyRequestType, int> RequestsAttempted { get; }

        public void Attempt(ApiKeyRequestType type)
        {
            RequestsAttempted.AddOrUpdate(type, 1, (key, old) => old + 1);
        }

        public ConcurrentDictionary<ApiKeyRequestType, int> Requests { get; }

        private long _duration = 0L;
        public TimeSpan Duration => TimeSpan.FromTicks(_duration);

        public void Success(ApiKeyRequestType type, TimeSpan duration)
        {
            Requests.AddOrUpdate(type, 1, (key, old) => old + 1);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        public ConcurrentDictionary<ApiKeyRequestType, int> RequestsFailed { get; }

        public void Failure(ApiKeyRequestType type, TimeSpan duration)
        {
            RequestsFailed.AddOrUpdate(type, 1, (key, old) => old + 1);
            Interlocked.Add(ref _duration, duration.Ticks);
        }

        private int _messages = 0;
        public int Messages => _messages;
        private int _messageBytes = 0;
        public int MessageBytes => _messageBytes;
        private int _messageTcpBytes = 0;
        public int MessageTcpBytes => _messageTcpBytes;

        public void Produce(int messages, int wireBytes, int bytesCompressed)
        {
            Interlocked.Add(ref _messages, messages);
            Interlocked.Add(ref _messageTcpBytes, wireBytes);
            Interlocked.Add(ref _messageBytes, wireBytes + bytesCompressed);
        }
    }
}