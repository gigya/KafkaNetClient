using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient.Tests
{
    public class FakeConnection : IConnection
    {
        public Func<Task<ProduceResponse>> ProduceResponseFunction;
        public Func<Task<MetadataResponse>> MetadataResponseFunction;
        public Func<Task<OffsetResponse>> OffsetResponseFunction;
        public Func<Task<FetchResponse>> FetchResponseFunction;
        public Func<Task<GroupCoordinatorResponse>> GroupCoordinatorResponseFunction;

        public FakeConnection(Uri address)
        {
            Endpoint = new ConnectionFactory().Resolve(address, TestConfig.Log);
        }

        public void ResetRequestCallCount(ApiKeyRequestType requestType)
        {
            long value;
            _requestCounts.TryRemove(requestType, out value);
        }

        public long RequestCallCount(ApiKeyRequestType requestType)
        {
            long count;
            return _requestCounts.TryGetValue(requestType, out count) ? count : 0L;
        }

        private readonly ConcurrentDictionary<ApiKeyRequestType, long> _requestCounts = new ConcurrentDictionary<ApiKeyRequestType, long>();

        public Endpoint Endpoint { get; }

        public bool IsReaderAlive => true;

        public Task SendAsync(DataPayload payload, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        /// <exception cref="Exception">A delegate callback throws an exception.</exception>
        public async Task<T> SendAsync<T>(IRequest<T> request, CancellationToken token, IRequestContext context = null) where T : class, IResponse
        {
            _requestCounts.AddOrUpdate(request.ApiKey, 1L, (type, current) => current + 1);

            if (typeof (T) == typeof (ProduceResponse)) return (T) (object) await ProduceResponseFunction();
            if (typeof (T) == typeof (MetadataResponse)) return (T) (object) await MetadataResponseFunction();
            if (typeof (T) == typeof (OffsetResponse)) return (T) (object) await OffsetResponseFunction();
            if (typeof (T) == typeof (FetchResponse)) return (T) (object) await FetchResponseFunction();
            if (typeof (T) == typeof (GroupCoordinatorResponse)) return (T) (object) await GroupCoordinatorResponseFunction();

            throw new NotImplementedException(typeof(T).FullName);
        }

        public void Dispose()
        {
        }
    }
}