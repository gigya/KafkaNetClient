using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using Nito.AsyncEx;

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
    public class Router : IRouter
    {
        private readonly IConnectionFactory _connectionFactory;
        private readonly IPartitionSelector _partitionSelector;

        private ImmutableDictionary<Endpoint, IConnection> _allConnections = ImmutableDictionary<Endpoint, IConnection>.Empty;
        private ImmutableDictionary<int, IConnection> _brokerConnections = ImmutableDictionary<int, IConnection>.Empty;
        private ImmutableDictionary<string, Tuple<MetadataResponse.Topic, DateTimeOffset>> _topicCache = ImmutableDictionary<string, Tuple<MetadataResponse.Topic, DateTimeOffset>>.Empty;
        private ImmutableDictionary<string, Tuple<int, DateTimeOffset>> _groupCache = ImmutableDictionary<string, Tuple<int, DateTimeOffset>>.Empty;

        private readonly AsyncLock _lock = new AsyncLock();

        /// <exception cref="ConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public Router(KafkaOptions options)
            : this(options.ServerUris, options.ConnectionFactory, options.ConnectionConfiguration, options.PartitionSelector, options.RouterConfiguration, options.Log)
        {
        }

        /// <exception cref="ConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public Router(Uri serverUri, IConnectionFactory connectionFactory = null, IConnectionConfiguration connectionConfiguration = null, IPartitionSelector partitionSelector = null, IRouterConfiguration routerConfiguration = null, ILog log = null)
            : this (new []{ serverUri }, connectionFactory, connectionConfiguration, partitionSelector, routerConfiguration, log)
        {
        }

        /// <exception cref="ConnectionException">None of the provided Kafka servers are resolvable.</exception>
        public Router(IEnumerable<Uri> serverUris, IConnectionFactory connectionFactory = null, IConnectionConfiguration connectionConfiguration = null, IPartitionSelector partitionSelector = null, IRouterConfiguration routerConfiguration = null, ILog log = null)
        {
            Log = log ?? TraceLog.Log;
            ConnectionConfiguration = connectionConfiguration ?? new ConnectionConfiguration();
            _connectionFactory = connectionFactory ?? new ConnectionFactory();

            foreach (var uri in serverUris) {
                try {
                    var endpoint = _connectionFactory.Resolve(uri, Log);
                    var connection = _connectionFactory.Create(endpoint, ConnectionConfiguration, Log);
                    _allConnections = _allConnections.SetItem(endpoint, connection);
                } catch (ConnectionException ex) {
                    Log.Warn(() => LogEvent.Create(ex, $"Ignoring uri that could not be resolved: {uri}"));
                }
            }

            if (_allConnections.IsEmpty) throw new ConnectionException("None of the provided Kafka servers are resolvable.");

            Configuration = routerConfiguration ?? new RouterConfiguration();
            _partitionSelector = partitionSelector ?? new PartitionSelector();
        }

        public IConnectionConfiguration ConnectionConfiguration { get; }
        public IRouterConfiguration Configuration { get; }

        /// <inheritdoc />
        public IEnumerable<IConnection> Connections => _allConnections.Values;

        /// <inheritdoc />
        public TopicBroker GetTopicBroker(string topicName, int partitionId)
        {
            return GetCachedTopicBroker(topicName, partitionId, GetCachedTopic(topicName));
        }

        private TopicBroker GetCachedTopicBroker(string topicName, int partitionId, MetadataResponse.Topic topic)
        {
            var partition = topic.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null)
                throw new CachedMetadataException($"The topic ({topicName}) has no partitionId {partitionId} defined.") {
                    TopicName = topicName,
                    Partition = partitionId
                };

            return GetCachedTopicBroker(topicName, partition);
        }

        /// <inheritdoc />
        public TopicBroker GetTopicBroker(string topicName, byte[] key)
        {
            var topic = GetCachedTopic(topicName);
            return GetCachedTopicBroker(topicName, _partitionSelector.Select(topic, key));
        }

        /// <inheritdoc />
        public async Task<TopicBroker> GetTopicBrokerAsync(string topicName, int partitionId, CancellationToken cancellationToken)
        {
            return GetCachedTopicBroker(topicName, partitionId, await GetTopicMetadataAsync(topicName, cancellationToken));
        }

        /// <inheritdoc />
        public MetadataResponse.Topic GetTopicMetadata(string topicName)
        {
            return GetCachedTopic(topicName);
        }

        /// <inheritdoc />
        public IImmutableList<MetadataResponse.Topic> GetTopicMetadata(IEnumerable<string> topicNames)
        {
            var topicSearchResult = TryGetCachedTopics(topicNames);
            if (topicSearchResult.Missing.Count > 0) throw new CachedMetadataException($"No metadata defined for topics: {string.Join(",", topicSearchResult.Missing)}");

            return ImmutableList<MetadataResponse.Topic>.Empty.AddRange(topicSearchResult.Topics);
        }

        /// <inheritdoc />
        public IImmutableList<MetadataResponse.Topic> GetTopicMetadata()
        {
            return ImmutableList<MetadataResponse.Topic>.Empty.AddRange(_topicCache.Values.Select(t => t.Item1));
        }

        /// <inheritdoc />
        public async Task<MetadataResponse.Topic> GetTopicMetadataAsync(string topicName, CancellationToken cancellationToken)
        {
            return TryGetCachedTopic(topicName) 
                ?? await UpdateTopicMetadataFromServerAsync(topicName, true, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<IImmutableList<MetadataResponse.Topic>> GetTopicMetadataAsync(IEnumerable<string> topicNames, CancellationToken cancellationToken)
        {
            var searchResult = TryGetCachedTopics(topicNames);
            return searchResult.Missing.Count == 0 
                ? searchResult.Topics 
                : searchResult.Topics.AddRange(await UpdateTopicMetadataFromServerAsync(searchResult.Missing, false, cancellationToken).ConfigureAwait(false));
        }

        /// <inheritdoc />
        public Task RefreshTopicMetadataAsync(string topicName, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync(topicName, ignoreCacheExpiry, cancellationToken);
        }

        /// <inheritdoc />
        public Task RefreshTopicMetadataAsync(IEnumerable<string> topicNames, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync(topicNames, ignoreCacheExpiry, cancellationToken);
        }

        /// <inheritdoc />
        public Task RefreshTopicMetadataAsync(CancellationToken cancellationToken)
        {
            return UpdateTopicMetadataFromServerAsync((IEnumerable<string>) null, true, cancellationToken);
        }

        private async Task<MetadataResponse.Topic> UpdateTopicMetadataFromServerAsync(string topicName, bool ignoreCache, CancellationToken cancellationToken)
        {
            var topics = await UpdateTopicMetadataFromServerAsync(new [] { topicName }, ignoreCache, cancellationToken).ConfigureAwait(false);
            return topics.SingleOrDefault();
        }

        private async Task<IImmutableList<MetadataResponse.Topic>> UpdateTopicMetadataFromServerAsync(IEnumerable<string> topicNames, bool ignoreCache, CancellationToken cancellationToken)
        {
            // TODO: more sophisticated locking should be particular to topicName(s) in that multiple 
            // requests can be made in parallel for different topicName(s).
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                var searchResult = new CachedTopicsResult(missing: topicNames);
                if (!ignoreCache) {
                    searchResult = TryGetCachedTopics(searchResult.Missing, Configuration.CacheExpiration);
                    if (searchResult.Missing.Count == 0) return searchResult.Topics;
                }

                MetadataRequest request;
                MetadataResponse response;
                if (ignoreCache && topicNames == null) {
                    Log.Info(() => LogEvent.Create("Router refreshing metadata for all topics"));
                    request = new MetadataRequest();
                    response = await this.GetMetadataAsync(request, cancellationToken);
                } else {
                    Log.Info(() => LogEvent.Create($"Router refreshing metadata for topics {string.Join(",", searchResult.Missing)}"));
                    request = new MetadataRequest(searchResult.Missing);
                    response = await this.GetMetadataAsync(request, cancellationToken);
                }

                if (ignoreCache && (response?.Topics == null || response.Topics.Count == 0)) {
                    throw new CachedMetadataException($"Unable to refresh metadata for topics {string.Join(",", request.Topics)}");
                }

                UpdateConnectionCache(response);
                UpdateTopicCache(response);

                // since the above may take some time to complete, it's necessary to hold on to the topics we found before
                // just in case they expired between when we searched for them and now.
                var result = searchResult.Topics.AddNotNullRange(response?.Topics);
                return result;
            }
        }

        private async Task<int> UpdateGroupMetadataFromServerAsync(string groupId, bool ignoreCache, CancellationToken cancellationToken)
        {
            using (await _lock.LockAsync(cancellationToken).ConfigureAwait(false)) {
                if (!ignoreCache) {
                    var brokerId = TryGetCachedGroupBrokerId(groupId, Configuration.CacheExpiration);
                    if (brokerId.HasValue) return brokerId.Value;
                }

                Log.Info(() => LogEvent.Create($"Router refreshing metadata for group {groupId}"));
                var request = new GroupCoordinatorRequest(groupId);
                try {
                    var response = await this.SendToAnyAsync(request, cancellationToken);

                    UpdateConnectionCache(new [] { response });
                    UpdateGroupCache(request, response);

                    return response.BrokerId;
                } catch (Exception ex) {
                    throw new CachedMetadataException($"Unable to refresh metadata for group {groupId}", ex);
                }
            }
        }

        private CachedTopicsResult TryGetCachedTopics(IEnumerable<string> topicNames, TimeSpan? expiration = null)
        {
            var missing = new List<string>();
            var topics = new List<MetadataResponse.Topic>();

            foreach (var topicName in topicNames.Distinct()) {
                var topic = TryGetCachedTopic(topicName, expiration);
                if (topic != null) {
                    topics.Add(topic);
                } else {
                    missing.Add(topicName);
                }
            }

            return new CachedTopicsResult(topics, missing);
        }

        private MetadataResponse.Topic GetCachedTopic(string topicName, TimeSpan? expiration = null)
        {
            var topic = TryGetCachedTopic(topicName, expiration);
            if (topic != null) return topic;

            throw new CachedMetadataException($"No metadata defined for topic/{topicName}") { TopicName = topicName };
        }

        private MetadataResponse.Topic TryGetCachedTopic(string topicName, TimeSpan? expiration = null)
        {
            Tuple<MetadataResponse.Topic, DateTimeOffset> cachedValue;
            if (_topicCache.TryGetValue(topicName, out cachedValue) && !HasExpired(cachedValue, expiration)) {
                return cachedValue.Item1;
            }
            return null;
        }

        private static bool HasExpired<T>(Tuple<T, DateTimeOffset> cachedValue, TimeSpan? expiration = null)
        {
            return expiration.HasValue && expiration.Value < DateTimeOffset.UtcNow - cachedValue.Item2;
        }

        private TopicBroker GetCachedTopicBroker(string topicName, MetadataResponse.Partition partition)
        {
            IConnection conn;
            if (_brokerConnections.TryGetValue(partition.LeaderId, out conn)) {
                return new TopicBroker(topicName, partition.PartitionId, partition.LeaderId, conn);
            }

            throw new CachedMetadataException($"Lead broker cannot be found for partition/{partition.PartitionId}, leader {partition.LeaderId}") {
                TopicName = topicName,
                Partition = partition.PartitionId
            };
        }

        private GroupBroker GetCachedGroupBroker(string groupId, int brokerId)
        {
            IConnection conn;
            if (_brokerConnections.TryGetValue(brokerId, out conn)) {
                return new GroupBroker(groupId, brokerId, conn);
            }

            throw new CachedMetadataException($"Broker cannot be found for group/{groupId}, broker {brokerId}");
        }

        private int GetCachedGroupBrokerId(string groupId, TimeSpan? expiration = null)
        {
            var brokerId = TryGetCachedGroupBrokerId(groupId, expiration);
            if (brokerId.HasValue) return brokerId.Value;

            throw new CachedMetadataException($"No metadata defined for group/{groupId}");
        }

        private int? TryGetCachedGroupBrokerId(string groupId, TimeSpan? expiration = null)
        {
            Tuple<int, DateTimeOffset> cachedValue;
            if (_groupCache.TryGetValue(groupId, out cachedValue) && !HasExpired(cachedValue, expiration)) {
                return cachedValue.Item1;
            }
            return null;
        }

        private CachedMetadataException GetPartitionElectionException(IList<TopicPartition> partitionElections)
        {
            var topic = partitionElections.FirstOrDefault();
            if (topic == null) return null;

            var message = $"Leader Election for topic {topic.TopicName} partition {topic.PartitionId}";
            var innerException = GetPartitionElectionException(partitionElections.Skip(1).ToList());
            var exception = innerException != null
                                ? new CachedMetadataException(message, innerException)
                                : new CachedMetadataException(message);
            exception.TopicName = topic.TopicName;
            exception.Partition = topic.PartitionId;
            return exception;
        }

        private void UpdateGroupCache(GroupCoordinatorRequest request, GroupCoordinatorResponse response)
        {
            if (request == null || response == null) return;

            _groupCache = _groupCache.SetItem(request.GroupId, new Tuple<int, DateTimeOffset>(response.BrokerId, DateTimeOffset.UtcNow));
        }

        private void UpdateTopicCache(MetadataResponse metadata)
        {
            if (metadata == null) return;

            var partitionElections = metadata.Topics.SelectMany(
                t => t.Partitions
                      .Where(p => p.IsElectingLeader)
                      .Select(p => new TopicPartition(t.TopicName, p.PartitionId)))
                      .ToList();
            if (partitionElections.Any()) throw GetPartitionElectionException(partitionElections);

            var topicCache = _topicCache;
            try {
                foreach (var topic in metadata.Topics) {
                    topicCache = topicCache.SetItem(topic.TopicName, new Tuple<MetadataResponse.Topic, DateTimeOffset>(topic, DateTimeOffset.UtcNow));
                }
            } finally {
                _topicCache = topicCache;
            }
        }

        private void UpdateConnectionCache(MetadataResponse metadata)
        {
            if (metadata == null) return;

            UpdateConnectionCache(metadata.Brokers);
        }

        private void UpdateConnectionCache(IEnumerable<Protocol.Broker> brokers)
        {
            var allConnections = _allConnections;
            var brokerConnections = _brokerConnections;
            var connectionsToDispose = ImmutableList<IConnection>.Empty;
            try {
                foreach (var broker in brokers) {
                    var endpoint = _connectionFactory.Resolve(new Uri($"http://{broker.Host}:{broker.Port}"), Log);

                    IConnection connection;
                    if (brokerConnections.TryGetValue(broker.BrokerId, out connection)) {
                        if (connection.Endpoint.Equals(endpoint)) {
                            // existing connection, nothing to change
                        } else {
                            // ReSharper disable once AccessToModifiedClosure
                            Log.Warn(() => LogEvent.Create($"Broker {broker.BrokerId} Uri changed from {connection.Endpoint} to {endpoint}"));

                            // A connection changed for a broker, so close the old connection and create a new one
                            connectionsToDispose = connectionsToDispose.Add(connection);
                            connection = _connectionFactory.Create(endpoint, ConnectionConfiguration, Log);
                            // important that we create it here rather than set to null or we'll get it again from allConnections
                        }
                    }

                    if (connection == null && !allConnections.TryGetValue(endpoint, out connection)) {
                        connection = _connectionFactory.Create(endpoint, ConnectionConfiguration, Log);
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

        private void DisposeConnections(IEnumerable<IConnection> connections)
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

        /// <inheritdoc />
        public ILog Log { get; }

        /// <inheritdoc />
        public GroupBroker GetGroupBroker(string groupId)
        {
            return GetCachedGroupBroker(groupId, GetCachedGroupBrokerId(groupId));
        }

        /// <inheritdoc />
        public async Task<GroupBroker> GetGroupBrokerAsync(string groupId, CancellationToken cancellationToken)
        {
            return GetCachedGroupBroker(groupId, await GetGroupBrokerIdAsync(groupId, cancellationToken));
        }

        /// <inheritdoc />
        public async Task<int> GetGroupBrokerIdAsync(string groupId, CancellationToken cancellationToken)
        {
            return TryGetCachedGroupBrokerId(groupId) 
                ?? await UpdateGroupMetadataFromServerAsync(groupId, true, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public Task RefreshGroupMetadataAsync(string groupId, bool ignoreCacheExpiry, CancellationToken cancellationToken)
        {
            return UpdateGroupMetadataFromServerAsync(groupId, ignoreCacheExpiry, cancellationToken);
        }

        private class CachedTopicsResult
        {
            public IImmutableList<MetadataResponse.Topic> Topics { get; }
            public IImmutableList<string> Missing { get; }

            public CachedTopicsResult(IEnumerable<MetadataResponse.Topic> topics = null, IEnumerable<string> missing = null)
            {
                Topics = ImmutableList<MetadataResponse.Topic>.Empty.AddNotNullRange(topics);
                Missing = ImmutableList<string>.Empty.AddNotNullRange(missing);
            }
        }
    }
}