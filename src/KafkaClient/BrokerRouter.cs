﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connection;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// This class provides an abstraction from querying multiple Kafka servers for Metadata details and caching this data.
    ///
    /// All metadata queries are cached lazily.  If metadata from a topic does not exist in cache it will be queried for using
    /// the default brokers provided in the constructor.  Each Uri will be queried to get metadata information in turn until a
    /// response is received.  It is recommended therefore to provide more than one Kafka Uri as this API will be able to to get
    /// metadata information even if one of the Kafka servers goes down.
    ///
    /// The metadata will stay in cache until an error condition is received indicating the metadata is out of data.  This error
    /// can be in the form of a socket disconnect or an error code from a response indicating a broker no longer hosts a partition.
    /// </summary>
    public class BrokerRouter : IBrokerRouter
    {
        private readonly KafkaMetadataProvider _kafkaMetadataProvider;
        private readonly IKafkaConnectionFactory _connectionFactory;
        private readonly IKafkaConnectionConfiguration _connectionConfiguration;
        private readonly ICacheConfiguration _cacheConfiguration;
        private readonly IPartitionSelector _partitionSelector;

        private ImmutableDictionary<KafkaEndpoint, IKafkaConnection> _allConnections = ImmutableDictionary<KafkaEndpoint, IKafkaConnection>.Empty;
        private ImmutableDictionary<int, IKafkaConnection> _brokerConnections = ImmutableDictionary<int, IKafkaConnection>.Empty;
        private ImmutableDictionary<string, Tuple<MetadataTopic, DateTime>> _topicCache = ImmutableDictionary<string, Tuple<MetadataTopic, DateTime>>.Empty;

        private readonly AsyncLock _lock = new AsyncLock();

        /// <exception cref="KafkaConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public BrokerRouter(KafkaOptions options)
            : this(options.ServerUris, options.ConnectionFactory, options.ConnectionConfiguration, options.PartitionSelector, options.CacheConfiguration, options.Log)
        {
        }

        /// <exception cref="KafkaConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public BrokerRouter(Uri serverUri, IKafkaConnectionFactory connectionFactory = null, IKafkaConnectionConfiguration connectionConfiguration = null, IPartitionSelector partitionSelector = null, ICacheConfiguration cacheConfiguration = null, IKafkaLog log = null)
            : this (new []{ serverUri }, connectionFactory, connectionConfiguration, partitionSelector, cacheConfiguration, log)
        {
        }

        /// <exception cref="KafkaConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public BrokerRouter(IEnumerable<Uri> serverUris, IKafkaConnectionFactory connectionFactory = null, IKafkaConnectionConfiguration connectionConfiguration = null, IPartitionSelector partitionSelector = null, ICacheConfiguration cacheConfiguration = null, IKafkaLog log = null)
        {
            Log = log ?? TraceLog.Log;
            _connectionConfiguration = connectionConfiguration ?? new KafkaConnectionConfiguration();
            _connectionFactory = connectionFactory ?? new KafkaConnectionFactory();

            foreach (var uri in serverUris) {
                try {
                    var endpoint = _connectionFactory.Resolve(uri, Log);
                    var connection = _connectionFactory.Create(endpoint, _connectionConfiguration, Log);
                    _allConnections = _allConnections.SetItem(endpoint, connection);
                } catch (KafkaConnectionException ex) {
                    Log.WarnFormat(ex, "Ignoring uri that could not be resolved: {0}", uri);
                }
            }

            if (_allConnections.IsEmpty) throw new KafkaConnectionException("None of the provided Kafka servers are resolvable.");

            _cacheConfiguration = cacheConfiguration ?? new CacheConfiguration();
            _partitionSelector = partitionSelector ?? new PartitionSelector();
            _kafkaMetadataProvider = new KafkaMetadataProvider(Log);
        }

        /// <summary>
        /// Select a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topicName">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria.  If the given partitionId does not exist an exception will be thrown.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the given topic or partitionId does not exist for the given topic.</exception>
        public BrokerRoute GetBrokerRoute(string topicName, int partitionId)
        {
            return GetBrokerRoute(topicName, partitionId, GetCachedTopic(topicName));
        }

        private BrokerRoute GetBrokerRoute(string topicName, int partitionId, MetadataTopic topic)
        {
            var partition = topic.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null)
                throw new CachedMetadataException($"The topic ({topicName}) has no partitionId {partitionId} defined.") {
                    Topic = topicName,
                    Partition = partitionId
                };

            return GetCachedRoute(topicName, partition);
        }

        /// <summary>
        /// Select a broker for a given topic using the IPartitionSelector function.
        /// </summary>
        /// <param name="topicName">The topic to retreive a broker route for.</param>
        /// <param name="key">The key used by the IPartitionSelector to collate to a consistent partition. Null value means key will be ignored in selection process.</param>
        /// <returns>A broker route for the given topic.</returns>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        public BrokerRoute GetBrokerRoute(string topicName, byte[] key = null)
        {
            var topic = GetCachedTopic(topicName);
            return GetCachedRoute(topicName, _partitionSelector.Select(topic, key));
        }

        /// <summary>
        /// Get a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topicName">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria. This method will check the cache first, and if the topic and partition
        /// is missing it will initiate a call to the kafka servers, updating the cache with the resulting metadata.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the given topic or partitionId does not exist for the given topic even after a refresh.</exception>
        public async Task<BrokerRoute> GetBrokerRouteAsync(string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return GetBrokerRoute(topicName, partitionId, await GetTopicMetadataAsync(topicName, cancellationToken));
        }

        /// <summary>
        /// Returns Topic metadata for the given topic.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale, 
        /// use <see cref="GetTopicMetadataAsync(string, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        public MetadataTopic GetTopicMetadata(string topicName)
        {
            return GetCachedTopic(topicName);
        }

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <returns>List of Topics currently in the cache.</returns>
        /// <remarks>
        /// The topic metadata returned is from what is currently in the cache. To ensure data is not too stale, 
        /// use <see cref="GetTopicMetadataAsync(IEnumerable&lt;string&gt;, CancellationToken)"/>.
        /// </remarks>
        /// <exception cref="CachedMetadataException">Thrown if the topic metadata does not exist in the cache.</exception>
        public ImmutableList<MetadataTopic> GetTopicMetadata(IEnumerable<string> topicNames)
        {
            var topicSearchResult = TryGetCachedTopics(topicNames);
            if (topicSearchResult.Missing.Count > 0) throw new CachedMetadataException($"No metadata defined for topics: {string.Join(",", topicSearchResult.Missing)}");

            return ImmutableList<MetadataTopic>.Empty.AddRange(topicSearchResult.Topics);
        }

        /// <summary>
        /// Returns all cached topic metadata.
        /// </summary>
        public ImmutableList<MetadataTopic> GetTopicMetadata()
        {
            return ImmutableList<MetadataTopic>.Empty.AddRange(_topicCache.Values.Select(t => t.Item1));
        }

        /// <summary>
        /// Returns Topic metadata for the topic requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and if the topic is missing it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        public async Task<MetadataTopic> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken)
        {
            return TryGetCachedTopic(topicName) 
                ?? await UpdateTopicMetadataFromServerIfMissingAsync(topicName, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <remarks>
        /// This method will check the cache first, and for any missing topic metadata it will initiate a call to the kafka 
        /// servers, updating the cache with the resulting metadata.
        /// </remarks>
        public async Task<ImmutableList<MetadataTopic>> GetTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            var searchResult = TryGetCachedTopics(topicNames);
            return searchResult.Missing.IsEmpty 
                ? searchResult.Topics 
                : searchResult.Topics.AddRange(await UpdateTopicMetadataFromServerIfMissingAsync(searchResult.Missing, cancellationToken).ConfigureAwait(false));
        }

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given topic.
        /// </summary>
        /// <param name="topicName">The topic name to refresh metadata for.</param>
        /// <param name="ignoreCacheExpiry">Whether to refresh all data, or only data which has expired.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for the given topic, updating the cache with the resulting metadata.
        /// </remarks>
        public Task RefreshTopicMetadataAsync(string topicName, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return ignoreCacheExpiry 
                ? UpdateTopicMetadataFromServerAsync(topicName, cancellationToken)
                : UpdateTopicMetadataFromServerIfMissingAsync(topicName, cancellationToken);
        }

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for all topics.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for all topics, updating the cache with the resulting metadata.
        /// </remarks>
        public Task RefreshTopicMetadataAsync(CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync(null, cancellationToken);
        }

        private async Task<MetadataTopic> UpdateTopicMetadataFromServerIfMissingAsync(string topicName, CancellationToken cancellationToken)
        {
            var topics = await UpdateTopicMetadataFromServerIfMissingAsync(new [] { topicName }, cancellationToken).ConfigureAwait(false);
            return topics.Single();
        }

        private async Task<ImmutableList<MetadataTopic>> UpdateTopicMetadataFromServerIfMissingAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                var searchResult = TryGetCachedTopics(topicNames, _cacheConfiguration.CacheExpiration);
                if (searchResult.Missing.Count == 0) return searchResult.Topics;

                Log.DebugFormat("BrokerRouter refreshing metadata for topics {0}", string.Join(",", searchResult.Missing));
                var response = await GetTopicMetadataFromServerAsync(searchResult.Missing, cancellationToken);
                UpdateConnectionCache(response);
                UpdateTopicCache(response);

                // since the above may take some time to complete, it's necessary to hold on to the topics we found before
                // just in case they expired between when we searched for them and now.
                return response.Topics.AddRange(searchResult.Topics);
            }
        }

        private async Task UpdateTopicMetadataFromServerAsync(string topicName, CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                if (topicName != null) {
                    Log.DebugFormat("BrokerRouter refreshing metadata for topic {0}", topicName);
                } else {
                    Log.DebugFormat("BrokerRouter refreshing metadata for all topics");
                }
                var response = await GetTopicMetadataFromServerAsync(new [] { topicName }, cancellationToken);
                UpdateConnectionCache(response);
                UpdateTopicCache(response);
            }
        }

        private async Task<MetadataResponse> GetTopicMetadataFromServerAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            using (var cancellation = new TimedCancellation(cancellationToken, _cacheConfiguration.RefreshTimeout)) {
                var requestTask = topicNames != null
                        ? _kafkaMetadataProvider.GetAsync(_allConnections.Values, topicNames, cancellation.Token)
                        : _kafkaMetadataProvider.GetAsync(_allConnections.Values, cancellation.Token);
                return await requestTask.ConfigureAwait(false);
            }
        }

        private CachedTopicsResult TryGetCachedTopics(IEnumerable<string> topicNames, TimeSpan? expiration = null)
        {
            var missing = new List<string>();
            var topics = new List<MetadataTopic>();

            foreach (var topicName in topicNames) {
                var topic = TryGetCachedTopic(topicName, expiration);
                if (topic != null) {
                    topics.Add(topic);
                } else {
                    missing.Add(topicName);
                }
            }

            return new CachedTopicsResult(topics, missing);
        }

        private MetadataTopic GetCachedTopic(string topicName, TimeSpan? expiration = null)
        {
            var topic = TryGetCachedTopic(topicName, expiration);
            if (topic != null) return topic;

            throw new CachedMetadataException($"No metadata defined for topic: {topicName}") { Topic = topicName };
        }

        private MetadataTopic TryGetCachedTopic(string topicName, TimeSpan? expiration = null)
        {
            Tuple<MetadataTopic, DateTime> cachedTopic;
            if (_topicCache.TryGetValue(topicName, out cachedTopic)) {
                if (!expiration.HasValue || DateTime.UtcNow - cachedTopic.Item2 < expiration.Value) {
                    return cachedTopic.Item1;
                }
            }
            return null;
        }

        private BrokerRoute GetCachedRoute(string topicName, MetadataPartition partition)
        {
            var route = TryGetCachedRoute(topicName, partition);
            if (route != null) return route;

            throw new CachedMetadataException($"Lead broker cannot be found for partition: {partition.PartitionId}, leader: {partition.LeaderId}") {
                Topic = topicName,
                Partition = partition.PartitionId
            };
        }

        private BrokerRoute TryGetCachedRoute(string topicName, MetadataPartition partition)
        {
            IKafkaConnection conn;
            return _brokerConnections.TryGetValue(partition.LeaderId, out conn)
                ? new BrokerRoute(topicName, partition.PartitionId, conn) 
                : null;
        }

        private CachedMetadataException GetPartitionElectionException(IList<Topic> partitionElections)
        {
            var topic = partitionElections.FirstOrDefault();
            if (topic == null) return null;

            var message = $"Leader Election for topic {topic.TopicName} partition {topic.PartitionId}";
            var innerException = GetPartitionElectionException(partitionElections.Skip(1).ToList());
            var exception = innerException != null
                                ? new CachedMetadataException(message, innerException)
                                : new CachedMetadataException(message);
            exception.Topic = topic.TopicName;
            exception.Partition = topic.PartitionId;
            return exception;
        }

        private void UpdateTopicCache(MetadataResponse metadata)
        {
            var partitionElections = metadata.Topics.SelectMany(
                t => t.Partitions
                      .Where(p => p.IsElectingLeader)
                      .Select(p => new Topic(t.TopicName, p.PartitionId)))
                      .ToList();
            if (partitionElections.Any()) throw GetPartitionElectionException(partitionElections);

            var topicCache = _topicCache;
            try {
                foreach (var topic in metadata.Topics) {
                    topicCache = topicCache.SetItem(topic.TopicName, new Tuple<MetadataTopic, DateTime>(topic, DateTime.UtcNow));
                }
            } finally {
                _topicCache = topicCache;
            }
        }

        private void UpdateConnectionCache(MetadataResponse metadata)
        {
            var allConnections = _allConnections;
            var brokerConnections = _brokerConnections;
            var connectionsToDispose = ImmutableList<IKafkaConnection>.Empty;
            try {
                foreach (var broker in metadata.Brokers) {
                    var endpoint = _connectionFactory.Resolve(broker.Address, Log);

                    IKafkaConnection connection;
                    if (brokerConnections.TryGetValue(broker.BrokerId, out connection)) {
                        if (connection.Endpoint.Equals(endpoint)) {
                            // existing connection, nothing to change
                        } else {
                            Log.WarnFormat("Broker {0} Uri changed from {1} to {2}", broker.BrokerId, connection.Endpoint, endpoint);
                            
                            // A connection changed for a broker, so close the old connection and create a new one
                            connectionsToDispose = connectionsToDispose.Add(connection);
                            connection = _connectionFactory.Create(endpoint, _connectionConfiguration, Log);
                            // important that we create it here rather than set to null or we'll get it again from allConnections
                        }
                    }

                    if (connection == null && !allConnections.TryGetValue(endpoint, out connection)) {
                        connection = _connectionFactory.Create(endpoint, _connectionConfiguration, Log);
                    }

                    allConnections = allConnections.SetItem(endpoint, connection);
                    brokerConnections = brokerConnections.SetItem(broker.BrokerId, connection);
                }
            } finally {
                _allConnections = allConnections;
                _brokerConnections = brokerConnections;
                DisposeConnections(connectionsToDispose);
            }
        }

        private void DisposeConnections(IEnumerable<IKafkaConnection> connections)
        {
            foreach (var connection in connections) {
                using (connection) {
                }
            }
        }

        private int _disposeCount;

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            DisposeConnections(_allConnections.Values);
        }

        public IKafkaLog Log { get; }

        private class CachedTopicsResult
        {
            public ImmutableList<MetadataTopic> Topics { get; }
            public ImmutableList<string> Missing { get; }

            public CachedTopicsResult(IEnumerable<MetadataTopic> topics, IEnumerable<string> missing)
            {
                Topics = ImmutableList<MetadataTopic>.Empty.AddNotNullRange(topics);
                Missing = ImmutableList<string>.Empty.AddNotNullRange(missing);
            }
        }
    }
}