using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    internal abstract class RoutedRequest<T> where T : class, IResponse
    {
        protected RoutedRequest(IRequest<T> request, ILog log)
        {
            _request = request;
            _log = log;
        }

        private readonly ILog _log;
        private readonly IRequest<T> _request;

        private Broker _route;
        private T _response;

        public async Task SendAsync(IRouter router, CancellationToken cancellationToken, IRequestContext context = null)
        {
            _response = null;
            _route = await GetBrokerAsync(router, cancellationToken).ConfigureAwait(false);
            _response = _route == null ? null : await _route.Connection.SendAsync(_request, cancellationToken, context).ConfigureAwait(false);
        }

        protected abstract Task<Broker> GetBrokerAsync(IRouter router, CancellationToken cancellationToken);

        public RetryAttempt<T> MetadataRetryResponse(int attempt, out bool? metadataInvalid)
        {
            metadataInvalid = false;
            if (_response == null) return _request.ExpectResponse ? RetryAttempt<T>.Retry : new RetryAttempt<T>(null);

            var errors = _response.Errors.Where(e => e != ErrorResponseCode.None).ToList();
            if (errors.Count == 0) return new RetryAttempt<T>(_response);

            var shouldRetry = errors.Any(e => e.IsRetryable());
            metadataInvalid = errors.All(e => e.IsFromStaleMetadata());
            _log.Warn(() => LogEvent.Create($"{_request.ApiKey} response contained errors (attempt {attempt}): {string.Join(" ", errors)}"));

            if (!shouldRetry) ThrowExtractedException(attempt);
            return RetryAttempt<T>.Retry;
        }

        public void MetadataRetry(int attempt, TimeSpan retry)
        {
            bool? ignored;
            MetadataRetry(attempt, null, out ignored);
        }

        public void MetadataRetry(int attempt, Exception exception, out bool? retry)
        {
            retry = true;
            _log.Debug(() => LogEvent.Create(exception, $"Retrying request {_request.ApiKey} (attempt {attempt})"));
            if (exception != null) {
                if (!exception.IsPotentiallyRecoverableByMetadataRefresh()) throw exception.PrepareForRethrow();
                retry = null; // ie. the state of the metadata is unknown
            }
        }

        public void ThrowExtractedException(int attempt)
        {
            throw ResponseException;
        }

        public Exception ResponseException => _request.ExtractExceptions(_response, _route.Connection.Endpoint);
    }
}