using System;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient
{
    public class ConsumerConfiguration : IConsumerConfiguration
    {
        public ConsumerConfiguration(
            TimeSpan? maxServerWait = null, 
            int? minFetchBytes = null, 
            int? maxFetchBytes = null, 
            int? maxPartitionFetchBytes = null,
            TimeSpan? heartbeatTimeout = null,
            TimeSpan? rebalanceTimeout = null,
            string protocolType = null,
            IRetry coordinationRetry = null)
        {
            MaxFetchServerWait = maxServerWait;
            MinFetchBytes = minFetchBytes;
            MaxFetchBytes = maxFetchBytes;
            MaxPartitionFetchBytes = maxPartitionFetchBytes;
            GroupHeartbeat = heartbeatTimeout ?? TimeSpan.FromSeconds(Defaults.HeartbeatSeconds);
            GroupRebalanceTimeout = rebalanceTimeout ?? GroupHeartbeat;
            GroupProtocol = protocolType ?? Defaults.ProtocolType;
            GroupCoordinationRetry = coordinationRetry ?? Defaults.CoordinationRetry(GroupRebalanceTimeout);
        }

        /// <inheritdoc/>
        public int? MaxFetchBytes { get; }
        /// <inheritdoc/>
        public int? MaxPartitionFetchBytes { get; }
        /// <inheritdoc/>
        public int? MinFetchBytes { get; }
        /// <inheritdoc/>
        public TimeSpan? MaxFetchServerWait { get; }

        /// <inheritdoc/>
        public TimeSpan GroupRebalanceTimeout { get; }
        /// <inheritdoc/>
        public TimeSpan GroupHeartbeat { get; }
        /// <inheritdoc/>
        public IRetry GroupCoordinationRetry { get; }
        /// <inheritdoc/>
        public string GroupProtocol { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default <see cref="GroupProtocol"/>
            /// </summary>
            public const string ProtocolType = ConsumerEncoder.Protocol;

            /// <summary>
            /// The default <see cref="GroupHeartbeat"/> and <see cref="GroupRebalanceTimeout"/> seconds
            /// </summary>
            public const int HeartbeatSeconds = 60;

            /// <summary>
            /// The default <see cref="GroupCoordinationRetry"/> timeout
            /// </summary>
            public const int CoordinationTimeoutMinutes = 1;

            /// <summary>
            /// The default max retries for <see cref="GroupCoordinationRetry"/>
            /// </summary>
            public const int MaxCoordinationAttempts = 6;

            /// <summary>
            /// The default <see cref="GroupCoordinationRetry"/> backoff delay
            /// </summary>
            public const int GroupCoordinationRetryMilliseconds = 100;

            public static IRetry CoordinationRetry(TimeSpan? timeout = null)
            {
                return new BackoffRetry(
                    timeout ?? TimeSpan.FromMinutes(CoordinationTimeoutMinutes),
                    TimeSpan.FromMilliseconds(GroupCoordinationRetryMilliseconds), 
                    MaxCoordinationAttempts);
            }
        }
    }
}