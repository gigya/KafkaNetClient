using System;
using System.Collections.Immutable;
using KafkaClient.Connections;

namespace KafkaClient.Telemetry
{
    public class TelemetryTracker : ITrackEvents
    {
        private readonly TimeSpan _aggregationPeriod;
        private readonly int _maxStatistics;

        public TelemetryTracker(TimeSpan aggregationPeriod, int maxStatistics = 0)
        {
            _aggregationPeriod = aggregationPeriod;
            _maxStatistics = maxStatistics;
        }

        public ImmutableList<TcpStatistics> TcpReads => _tcpReads;
        private ImmutableList<TcpStatistics> _tcpReads = ImmutableList<TcpStatistics>.Empty;
        private readonly object _tcpReadLock = new object();
        private TcpStatistics GetTcpRead() => GetStatistics(_tcpReadLock, () => new TcpStatistics(DateTimeOffset.UtcNow, _aggregationPeriod), ref _tcpReads);
 
        public ImmutableList<TcpStatistics> TcpWrites => _tcpWrites;
        private ImmutableList<TcpStatistics> _tcpWrites = ImmutableList<TcpStatistics>.Empty;
        private readonly object _tcpWriteLock = new object();
        private TcpStatistics GetTcpWrite() => GetStatistics(_tcpWriteLock, () => new TcpStatistics(DateTimeOffset.UtcNow, _aggregationPeriod), ref _tcpWrites);

        public ImmutableList<ConnectionStatistics> TcpConnections => _tcpConnections;
        private ImmutableList<ConnectionStatistics> _tcpConnections = ImmutableList<ConnectionStatistics>.Empty;
        private readonly object _tcpConnectionLock = new object();
        private ConnectionStatistics GetTcpConnect() => GetStatistics(_tcpConnectionLock, () => new ConnectionStatistics(DateTimeOffset.UtcNow, _aggregationPeriod), ref _tcpConnections);

        public ImmutableList<ApiStatistics> ApiRequests => _apiRequests;
        private ImmutableList<ApiStatistics> _apiRequests = ImmutableList<ApiStatistics>.Empty;
        private readonly object _apiRequestLock = new object();
        private ApiStatistics GetApiRequests() => GetStatistics(_apiRequestLock, () => new ApiStatistics(DateTimeOffset.UtcNow, _aggregationPeriod), ref _apiRequests);

        private T GetStatistics<T>(object tLock, Func<T> producer, ref ImmutableList<T> telemetry) where T : Statistics
        {
            lock (tLock) {
                if (telemetry.IsEmpty) {
                    var first = producer();
                    telemetry = telemetry.Add(first);
                    return first;
                }
                var latest = telemetry[telemetry.Count - 1];
                if (DateTimeOffset.UtcNow < latest.EndedAt) return latest;

                var next = producer();
                telemetry = telemetry.Add(next);
                if (telemetry.Count > _maxStatistics) {
                    telemetry = telemetry.RemoveAt(0);
                }
                return next;
            }
        }

        public void Disconnected(Endpoint endpoint, Exception exception)
        {
            GetTcpConnect().Failure();
        }

        public void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
            GetTcpConnect().Attempt();
        }

        public void Connected(Endpoint endpoint, int attempt, TimeSpan elapsed)
        {
            GetTcpConnect().Success(elapsed);
        }

        public void WriteEnqueued(Endpoint endpoint, DataPayload payload)
        {
            GetTcpWrite().Attempt();
            GetApiRequests().Attempt(payload.ApiKey);
        }

        public void Writing(Endpoint endpoint, DataPayload payload)
        {
            GetTcpWrite().Start(payload.Buffer.Length);
        }

        public void Written(Endpoint endpoint, DataPayload payload, TimeSpan elapsed)
        {
            GetTcpWrite().Success(elapsed, payload.Buffer.Length);
            GetApiRequests().Success(payload.ApiKey, elapsed);
        }

        public void WriteFailed(Endpoint endpoint, DataPayload payload, TimeSpan elapsed, Exception exception)
        {
            GetTcpWrite().Failure(elapsed);
            GetApiRequests().Failure(payload.ApiKey, elapsed);
        }

        public void Reading(Endpoint endpoint, int size)
        {
            GetTcpRead().Attempt(size);
        }

        public void ReadingChunk(Endpoint endpoint, int size, int read, TimeSpan elapsed)
        {
            GetTcpRead().Start(size - read);
        }

        public void ReadChunk(Endpoint endpoint, int size, int remaining, int read, TimeSpan elapsed)
        {
        }

        public void Read(Endpoint endpoint, byte[] buffer, TimeSpan elapsed)
        {
            GetTcpRead().Success(elapsed, buffer.Length);
        }

        public void ReadFailed(Endpoint endpoint, int size, TimeSpan elapsed, Exception exception)
        {
            GetTcpRead().Failure(elapsed);
        }

        public void ProduceRequestMessages(int messages, int requestBytes, int compressedBytes)
        {
            GetApiRequests().Produce(messages, requestBytes, compressedBytes);
        }
    }
}