using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;
using KafkaClient.Connections;

namespace KafkaClient
{
    public class KafkaOptions
    {
        public KafkaOptions(IEnumerable<Uri> kafkaServerUris = null, 
            IConnectionConfiguration connectionConfiguration = null, 
            IRouterConfiguration routerConfiguration = null,
            IConnectionFactory connectionFactory = null,
            IPartitionSelector partitionSelector = null,
            IProducerConfiguration producerConfiguration = null, 
            IConsumerConfiguration consumerConfiguration = null, 
            ILog log = null)
        {
            ServerUris = ImmutableList<Uri>.Empty.AddNotNullRange(kafkaServerUris);
            RouterConfiguration = routerConfiguration ?? new RouterConfiguration();
            ConnectionConfiguration = connectionConfiguration ?? new ConnectionConfiguration();
            ConnectionFactory = connectionFactory ?? new ConnectionFactory();
            PartitionSelector = partitionSelector ?? new PartitionSelector();
            ProducerConfiguration = producerConfiguration ?? new ProducerConfiguration();
            ConsumerConfiguration = consumerConfiguration ?? new ConsumerConfiguration();
            Log = log ?? TraceLog.Log;
        }

        public KafkaOptions(Uri kafkaServerUri = null, 
            IConnectionConfiguration connectionConfiguration = null, 
            IRouterConfiguration routerConfiguration = null,
            IConnectionFactory connectionFactory = null,
            IPartitionSelector partitionSelector = null,
            IProducerConfiguration producerConfiguration = null, 
            IConsumerConfiguration consumerConfiguration = null, 
            ILog log = null)
            : this (ImmutableList<Uri>.Empty.AddNotNull(kafkaServerUri), connectionConfiguration, routerConfiguration, connectionFactory, partitionSelector, producerConfiguration, consumerConfiguration, log)
        {
        }

        /// <summary>
        /// Initial list of Uris to kafka servers, used to query for metadata from Kafka. More than one is recommended!
        /// </summary>
        public IImmutableList<Uri> ServerUris { get; }

        /// <summary>
        /// Connection backoff and retry settings.
        /// </summary>
        public IConnectionConfiguration ConnectionConfiguration { get; }

        /// <summary>
        /// Cache expiry and retry settings.
        /// </summary>
        public IRouterConfiguration RouterConfiguration { get; }

        /// <summary>
        /// Provides a factory for creating new kafka connections.
        /// </summary>
        public IConnectionFactory ConnectionFactory { get; }

        /// <summary>
        /// Selector function for routing messages to partitions. Default is key/hash and round robin.
        /// </summary>
        public IPartitionSelector PartitionSelector { get; }

        /// <summary>
        /// BatchSize and delay settings.
        /// </summary>
        public IProducerConfiguration ProducerConfiguration { get; }

        /// <summary>
        /// Retry and timeout settings.
        /// </summary>
        public IConsumerConfiguration ConsumerConfiguration { get; }

        /// <summary>
        /// Log object to record operational messages.
        /// </summary>
        public ILog Log { get; }
    }
}