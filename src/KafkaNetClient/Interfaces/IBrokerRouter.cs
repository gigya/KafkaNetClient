﻿using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaNet
{
    public interface IBrokerRouter : IDisposable
    {
        /// <summary>
        /// Select a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topic">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria.  If the given partitionId does not exist an exception will be thrown.
        /// </remarks>
        /// <exception cref="InvalidTopicMetadataException">Thrown if the returned metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="InvalidPartitionException">Thrown if the give partitionId does not exist for the given topic.</exception>
        /// <exception cref="ServerUnreachableException">Thrown if none of the Default Brokers can be contacted.</exception>
        BrokerRoute SelectBrokerRouteFromLocalCache(string topic, int partitionId);

        /// <summary>
        /// Select a broker for a given topic using the IPartitionSelector function.
        /// </summary>
        /// <param name="topic">The topic to retreive a broker route for.</param>
        /// <param name="key">The key used by the IPartitionSelector to collate to a consistent partition. Null value means key will be ignored in selection process.</param>
        /// <returns>A broker route for the given topic.</returns>
        /// <exception cref="InvalidTopicMetadataException">Thrown if the returned metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="ServerUnreachableException">Thrown if none of the Default Brokers can be contacted.</exception>
        BrokerRoute SelectBrokerRouteFromLocalCache(string topic, byte[] key = null);

        /// <summary>
        /// Returns Topic metadata for each topic requested.
        /// </summary>
        /// <param name="topics">Collection of topics to request metadata for.</param>
        /// <returns>List of Topics as provided by Kafka.</returns>
        /// <remarks>The topic metadata will by default check the cache first and then request metadata from the server if it does not exist in cache.</remarks>
        List<MetadataTopic> GetTopicMetadataFromLocalCache(params string[] topics);

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given topics.
        /// </summary>
        /// <param name="topics">List of topics to update metadata for.</param>
        /// <remarks>
        /// This method will initiate a call to the kafka servers and retrieve metadata for all given topics, updating the broke cache in the process.
        /// </remarks>
        Task<bool> RefreshTopicMetadata(params string[] topics);

        /// <summary>
        /// Returns Topic metadata for each topic.
        /// </summary>
        /// <returns>List of topics as provided by Kafka.</returns>
        /// <remarks>
        /// The topic metadata will by default check the cache.
        /// </remarks>
        List<MetadataTopic> GetAllTopicMetadataFromLocalCache();

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for all topics.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for all topics, updating the cache with the resulting metadata.
        /// </remarks>
        Task RefreshAllTopicMetadata();

        Task RefreshMissingTopicMetadata(params string[] topics);

        DateTime GetTopicMetadataRefreshTime(string topic);

        IKafkaLog Log { get; }
    }
}