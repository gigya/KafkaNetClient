﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class BrokerRouterExtensions
    {
        /// <summary>
        /// Get offsets for each partition from a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="maxOffsets">How many to get, at most.</param>
        /// <param name="offsetTime">
        /// Used to ask for all messages before a certain time (ms). There are two special values.
        /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset.
        /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </param>
        /// <param name="cancellationToken"></param>
        public static async Task<IImmutableList<OffsetTopic>> GetTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, int maxOffsets, long offsetTime, CancellationToken cancellationToken)
        {
            var topicMetadata = await brokerRouter.GetTopicMetadataAsync(topicName, cancellationToken).ConfigureAwait(false);

            // send the offset request to each partition leader
            var sendRequests = topicMetadata.Partitions
                .GroupBy(x => x.LeaderId)
                .Select(partitions => {
                    var partitionId = partitions.Select(_ => _.PartitionId).First();
                    var route = brokerRouter.GetBrokerRoute(topicName, partitionId);

                    var request = new OffsetRequest(partitions.Select(_ => new Offset(topicName, _.PartitionId, offsetTime, maxOffsets)));
                    return route.Connection.SendAsync(request, cancellationToken);
                }).ToArray();

            await Task.WhenAll(sendRequests).ConfigureAwait(false);
            return ImmutableList<OffsetTopic>.Empty.AddNotNullRange(sendRequests.SelectMany(x => x.Result.Topics));
        }

        /// <summary>
        /// Get offsets for each partition from a given topic.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicName">Name of the topic to get offset information from.</param>
        /// <param name="cancellationToken"></param>
        public static Task<IImmutableList<OffsetTopic>> GetTopicOffsetAsync(this IBrokerRouter brokerRouter, string topicName, CancellationToken cancellationToken)
        {
            return brokerRouter.GetTopicOffsetAsync(topicName, Offset.DefaultMaxOffsets, Offset.DefaultTime, cancellationToken);
        }

        /// <exception cref="CachedMetadataException">Thrown if the cached metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="FetchOutOfRangeException">Thrown if the fetch request is not valid.</exception>
        /// <exception cref="TimeoutException">Thrown if there request times out</exception>
        /// <exception cref="ConnectionException">Thrown in case of network error contacting broker (after retries), or if none of the default brokers can be contacted.</exception>
        /// <exception cref="RequestException">Thrown in case of an unexpected error in the request</exception>
        public static async Task<T> SendAsync<T>(this IBrokerRouter brokerRouter, IRequest<T> request, string topicName, int partition, CancellationToken cancellationToken, IRequestContext context = null, IRetry retryPolicy = null) where T : class, IResponse
        {
            var topicNames = new[] { topicName };
            var log = brokerRouter.Log;
            bool? metadataInvalid = false;
            T response = null;
            Endpoint endpoint = null;

            return await (retryPolicy ?? new Retry(TimeSpan.MaxValue, 3)).AttemptAsync(
                async (attempt, timer) => {
                    metadataInvalid = await brokerRouter.MetadataRetryRefresh(topicNames, metadataInvalid, cancellationToken);

                    // create request
                    var route = await brokerRouter.GetBrokerRouteAsync(topicName, partition, cancellationToken);
                    endpoint = route.Connection.Endpoint;

                    // make request
                    response = await route.Connection.SendAsync(request, cancellationToken, context).ConfigureAwait(false);
                    return request.MetadataRetryResponse(response, attempt, endpoint, log, out metadataInvalid);
                },
                (attempt, retry) => request.MetadataRetry(attempt, brokerRouter.Log),
                finalAttempt => { throw request.ExtractExceptions(response, endpoint); },
                (ex, attempt, retry) => metadataInvalid = request.MetadataRetry(attempt, brokerRouter.Log, ex),
                null, // do nothing on final exception -- will be rethrown
                cancellationToken);
        }

        private static async Task<bool> MetadataRetryRefresh(this IBrokerRouter brokerRouter, IEnumerable<string> topicNames, bool? metadataInvalid, CancellationToken cancellationToken)
        {
            if (metadataInvalid.GetValueOrDefault(true)) {
                // unknown metadata status should not force the issue
                await brokerRouter.RefreshTopicMetadataAsync(topicNames, metadataInvalid.GetValueOrDefault(), cancellationToken).ConfigureAwait(false);
            }
            return false;
        }

        private static bool? MetadataRetry<T>(this IRequest<T> request, int attempt, ILog log, Exception exception = null) where T : IResponse
        {
            log.Debug(() => LogEvent.Create(exception, $"Retrying request {request.ApiKey} (attempt {attempt})"));
            if (exception != null) {
                if (IsPotentiallyRecoverableByMetadataRefresh(exception)) {
                    return null; // ie. the state of the metadata is unknown
                }
                throw exception.PrepareForRethrow();
            }
            return true;
        }

        private static RetryAttempt<T> MetadataRetryResponse<T>(this IRequest<T> request, T response, int attempt, Endpoint endpoint, ILog log, out bool? metadataInvalid) where T : class, IResponse
        {
            metadataInvalid = false;
            if (response == null) return request.ExpectResponse ? RetryAttempt<T>.Retry : new RetryAttempt<T>(null);

            var errors = response.Errors.Where(e => e != ErrorResponseCode.None).ToList();
            if (errors.Count == 0) return new RetryAttempt<T>(response);

            var shouldRetry = errors.Any(e => e.IsRetryable());
            metadataInvalid = errors.All(e => e.IsFromStaleMetadata());
            log.Warn(() => LogEvent.Create($"{request.ApiKey} response contained errors (attempt {attempt}): {string.Join(" ", errors)}"));

            if (!shouldRetry) throw request.ExtractExceptions(response, endpoint);
            return RetryAttempt<T>.Retry;
        }

        private static bool IsPotentiallyRecoverableByMetadataRefresh(Exception exception)
        {
            return exception is FetchOutOfRangeException
                || exception is TimeoutException
                || exception is ConnectionException
                || exception is CachedMetadataException;
        }

        /// <summary>
        /// Given a collection of server connections, query for the topic metadata.
        /// </summary>
        /// <param name="brokerRouter">The router which provides the route and metadata.</param>
        /// <param name="topicNames">Topics to get metadata information for.</param>
        /// <param name="cancellationToken"></param>
        /// <remarks>
        /// Used by <see cref="BrokerRouter"/> internally. Broken out for better testability, but not intended to be used separately.
        /// </remarks>
        /// <returns>MetadataResponse validated to be complete.</returns>
        internal static async Task<MetadataResponse> GetMetadataAsync(this IBrokerRouter brokerRouter, IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            var request = new MetadataRequest(topicNames);

            return await brokerRouter.Configuration.RefreshRetry.AttemptAsync(
                async (attempt, timer) => {
                    var response = await brokerRouter.GetMetadataAsync(request, cancellationToken).ConfigureAwait(false);
                    if (response == null) return new RetryAttempt<MetadataResponse>(null);

                    var results = response.Brokers
                        .Select(ValidateBroker)
                        .Union(response.Topics.Select(ValidateTopic))
                        .Where(r => !r.IsValid.GetValueOrDefault())
                        .ToList();

                    var exceptions = results.Select(r => r.ToException()).Where(e => e != null).ToList();
                    if (exceptions.Count == 1) throw exceptions.Single();
                    if (exceptions.Count > 1) throw new AggregateException(exceptions);

                    if (results.Count == 0) return new RetryAttempt<MetadataResponse>(response);
                    foreach (var result in results.Where(r => !string.IsNullOrEmpty(r.Message))) {
                        brokerRouter.Log.Warn(() => LogEvent.Create(result.Message));
                    }

                    return RetryAttempt<MetadataResponse>.Retry;
                },
                (attempt, retry) => brokerRouter.Log.Warn(() => LogEvent.Create($"Failed metadata request on attempt {attempt}: Will retry in {retry}")),
                null, // return the failed response above, resulting in a null
                (ex, attempt, retry) => {
                    throw ex.PrepareForRethrow();
                },
                (ex, attempt) => brokerRouter.Log.Warn(() => LogEvent.Create(ex, $"Failed metadata request on attempt {attempt}")),
                cancellationToken);
        }

        private static async Task<MetadataResponse> GetMetadataAsync(this IBrokerRouter brokerRouter, MetadataRequest request, CancellationToken cancellationToken)
        {
            var servers = new List<string>();
            foreach (var connection in brokerRouter.Connections) {
                var server = connection.Endpoint?.ToString();
                try {
                    return await connection.SendAsync(request, cancellationToken).ConfigureAwait(false);
                } catch (Exception ex) {
                    servers.Add(server);
                    brokerRouter.Log.Warn(() => LogEvent.Create(ex, $"Failed to contact {server}: Trying next server"));
                }
            }

            throw new RequestException(request.ApiKey, ErrorResponseCode.None, $"Unable to make Metadata Request to any of {string.Join(" ", servers)}");
        }

        private class MetadataResult
        {
            public bool? IsValid { get; }
            public string Message { get; }
            private readonly ErrorResponseCode _errorCode;

            public Exception ToException()
            {
                if (IsValid.GetValueOrDefault(true)) return null;

                if (_errorCode == ErrorResponseCode.None) return new ConnectionException(Message);
                return new RequestException(ApiKeyRequestType.Metadata, _errorCode, Message);
            }

            public MetadataResult(ErrorResponseCode errorCode = ErrorResponseCode.None, bool? isValid = null, string message = null)
            {
                Message = message ?? "";
                _errorCode = errorCode;
                IsValid = isValid;
            }
        }

        private static MetadataResult ValidateBroker(Broker broker)
        {
            if (broker.BrokerId == -1)             return new MetadataResult(ErrorResponseCode.Unknown);
            if (string.IsNullOrEmpty(broker.Host)) return new MetadataResult(ErrorResponseCode.None, false, "Broker missing host information.");
            if (broker.Port <= 0)                  return new MetadataResult(ErrorResponseCode.None, false, "Broker missing port information.");
            return new MetadataResult(isValid: true);
        }

        private static MetadataResult ValidateTopic(MetadataTopic topic)
        {
            var errorCode = topic.ErrorCode;
            if (errorCode == ErrorResponseCode.None) return new MetadataResult(isValid: true);
            if (errorCode.IsRetryable()) return new MetadataResult(errorCode, null, $"topic/{topic.TopicName} returned error code of {errorCode}: Retrying");
            return new MetadataResult(errorCode, false, $"topic/{topic.TopicName} returned an error of {errorCode}");
        }
    }
}