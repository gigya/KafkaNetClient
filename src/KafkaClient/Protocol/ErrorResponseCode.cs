namespace KafkaClient.Protocol
{
    /// <summary>
    /// Enumeration of error codes that might be returned from a Kafka server
    /// 
    /// See http://kafka.apache.org/protocol.html#protocol_error_codes for details.
    /// </summary>
    public enum ErrorResponseCode : short
    {
        /// <summary>
        /// No error -- it worked!
        /// </summary>
        None = 0,

        /// <summary>
        /// The server experienced an unexpected error when processing the request
        /// </summary>
        Unknown = -1,

        /// <summary>
        /// The requested offset is not within the range of offsets maintained by the server.
        /// </summary>
        OffsetOutOfRange = 1,

        /// <summary>
        /// This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.
        /// </summary>
        CorruptMessage = 2,

        /// <summary>
        /// This server does not host this topic-partition.
        /// </summary>
        UnknownTopicOrPartition = 3,

        /// <summary>
        /// The requested fetch size is invalid (negative?).
        /// </summary>
        InvalidFetchSize = 4,

        /// <summary>
        /// There is no leader for this topic-partition as we are in the middle of a leadership election.
        /// </summary>
        LeaderNotAvailable = 5,

        /// <summary>
        /// This server is not the leader for that topic-partition. It indicates that the clients metadata is out of date.
        /// </summary>
        NotLeaderForPartition = 6,

        /// <summary>
        /// The request timed out (on the server).
        /// </summary>
        RequestTimedOut = 7,

        /// <summary>
        /// Internal error code for broker-to-broker communication: The broker is not available.
        /// </summary>
        BrokerNotAvailable = 8,

        /// <summary>
        /// The replica is not available for the requested topic-partition.
        /// </summary>
        ReplicaNotAvailable = 9,

        /// <summary>
        /// The request included a message larger than the max message size the server will accept.
        /// </summary>
        MessageTooLarge = 10,

        /// <summary>
        /// Internal error code for broker-to-broker communication: The controller moved to another broker.
        /// </summary>
        StaleControllerEpoch = 11,

        /// <summary>
        /// The metadata field of the offset request was too large.
        /// </summary>
        OffsetMetadataTooLarge = 12,

        /// <summary>
        /// The server disconnected before a response was received.
        /// </summary>
        NetworkException = 13,

        /// <summary>
        /// The coordinator is loading and hence can't process requests for this group.
        /// </summary>
        GroupLoadInProgress = 14,

        /// <summary>
        /// The group coordinator is not available.
        /// </summary>
        GroupCoordinatorNotAvailable = 15,

        /// <summary>
        /// This is not the correct coordinator for this group.
        /// </summary>
        NotCoordinatorForGroup = 16,

        /// <summary>
        /// The request attempted to perform an operation on an invalid topic.
        /// </summary>
        InvalidTopic = 17,

        /// <summary>
        /// The request included message batch larger than the configured segment size on the server.
        /// </summary>
        RecordListTooLarge = 18,

        /// <summary>
        /// Messages are rejected since there are fewer in-sync replicas than required.
        /// </summary>
        NotEnoughReplicas = 19,

        /// <summary>
        /// Messages are written to the log, but to fewer in-sync replicas than required.
        /// </summary>
        NotEnoughReplicasAfterAppend = 20,

        /// <summary>
        /// Produce request specified an invalid value for required acks.
        /// </summary>
        InvalidRequiredAcks = 21,

        /// <summary>
        /// Specified group generation id is not valid.
        /// </summary>
        IllegalGeneration = 22,

        /// <summary>
        /// The group member's supported protocols are incompatible with those of existing members.
        /// </summary>
        InconsistentGroupProtocol = 23,

        /// <summary>
        /// The configured groupId is invalid.
        /// </summary>
        InvalidGroupId = 24,

        /// <summary>
        /// The coordinator is not aware of this member.
        /// </summary>
        UnknownMemberId = 25,

        /// <summary>
        /// The session timeout is not within the range allowed by the broker (as configured
        /// by group.min.session.timeout.ms and group.max.session.timeout.ms).
        /// </summary>
        InvalidSessionTimeout = 26,

        /// <summary>
        /// The group is rebalancing, so a rejoin is needed.
        /// </summary>
        RebalanceInProgress = 27,

        /// <summary>
        /// The committing offset data size is not valid
        /// </summary>
        InvalidCommitOffsetSize = 28,

        /// <summary>
        /// Not authorized to access topic.
        /// </summary>
        TopicAuthorizationFailed = 29,

        /// <summary>
        /// Not authorized to access group.
        /// </summary>
        GroupAuthorizationFailed = 30,

        /// <summary>
        /// Cluster authorization failed.
        /// </summary>
        ClusterAuthorizationFailed = 31,

        /// <summary>
        /// The timestamp of the message is out of acceptable range.
        /// </summary>
        InvalidTimestamp = 32,

        /// <summary>
        /// The broker does not support the requested SASL mechanism.
        /// </summary>
        UnsupportedSaslMechanism = 33,

        /// <summary>
        /// Request is not valid given the current SASL state.
        /// </summary>
        IllegalSaslState = 34,

        /// <summary>
        /// The version of API is not supported.
        /// </summary>
        UnsupportedVersion = 35,

        /// <summary>
        /// Topic with this name already exists.
        /// </summary>
        TopicAlreadyExists = 36,

        /// <summary>
        /// Number of partitions is invalid.
        /// </summary>
        InvalidPartitions = 37,

        /// <summary>
        /// Replication-factor is invalid.
        /// </summary>
        InvalidReplicationFactor = 38,

        /// <summary>
        /// Replica assignment is invalid.
        /// </summary>
        InvalidReplicaAssignment = 39,

        /// <summary>
        /// Configuration is invalid.
        /// </summary>
        InvalidConfig = 40,

        /// <summary>
        /// This is not the correct controller for this cluster.
        /// </summary>
        NotController = 41,

        /// <summary>
        /// This most likely occurs because of a request being malformed by the client library or the message 
        /// was sent to an incompatible broker. See the broker logs for more details.
        /// </summary>
        InvalidRequest = 42,

        /// <summary>
        /// The message format version on the broker does not support the request.
        /// </summary>
        UnsupportedForMessageFormat = 43
    }
}