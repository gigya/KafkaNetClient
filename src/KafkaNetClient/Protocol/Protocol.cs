using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Runtime.Serialization;
using KafkaNet.Model;

namespace KafkaNet.Protocol
{
    /// <summary>
    /// Extension methods which allow compression of byte arrays
    /// </summary>
    public static class Compression
    {
        public static byte[] Zip(byte[] bytes)
        {
            using (var destination = new MemoryStream())
            using (var gzip = new GZipStream(destination, CompressionLevel.Fastest, false))
            {
                gzip.Write(bytes, 0, bytes.Length);
                gzip.Flush();
                gzip.Close();
                return destination.ToArray();
            }
        }

        public static byte[] Unzip(byte[] bytes)
        {
            using (var source = new MemoryStream(bytes))
            using (var destination = new MemoryStream())
            using (var gzip = new GZipStream(source, CompressionMode.Decompress, false))
            {
                gzip.CopyTo(destination);
                gzip.Flush();
                gzip.Close();
                return destination.ToArray();
            }
        }
    }

    /// <summary>
    /// Enumeration of numeric codes that the ApiKey in the request can take for each request types.
    /// </summary>
    public enum ApiKeyRequestType
    {
        Produce = 0,
        Fetch = 1,
        Offset = 2,
        MetaData = 3,
        OffsetCommit = 8,
        OffsetFetch = 9,
        ConsumerMetadataRequest = 10
    }

    /// <summary>
    /// Enumeration of error codes that might be returned from a Kafka server
    /// </summary>
    public enum ErrorResponseCode : short
    {
        /// <summary>
        /// No error--it worked!
        /// </summary>
        NoError = 0,

        /// <summary>
        /// An unexpected server error
        /// </summary>
        Unknown = -1,

        /// <summary>
        /// The requested offset is outside the range of offsets maintained by the server for the given topic/partition.
        /// </summary>
        OffsetOutOfRange = 1,

        /// <summary>
        /// This indicates that a message contents does not match its CRC
        /// </summary>
        InvalidMessage = 2,

        /// <summary>
        /// This request is for a topic or partition that does not exist on this broker.
        /// </summary>
        UnknownTopicOrPartition = 3,

        /// <summary>
        /// The message has a negative size
        /// </summary>
        InvalidMessageSize = 4,

        /// <summary>
        /// This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.
        /// </summary>
        LeaderNotAvailable = 5,

        /// <summary>
        /// This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.
        /// </summary>
        NotLeaderForPartition = 6,

        /// <summary>
        /// This error is thrown if the request exceeds the user-specified time limit in the request.
        /// </summary>
        RequestTimedOut = 7,

        /// <summary>
        /// This is not a client facing error and is used only internally by intra-cluster broker communication.
        /// </summary>
        BrokerNotAvailable = 8,

        /// <summary>
        /// If replica is expected on a broker, but is not.
        /// </summary>
        ReplicaNotAvailable = 9,

        /// <summary>
        /// The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.
        /// </summary>
        MessageSizeTooLarge = 10,

        /// <summary>
        /// Internal error code for broker-to-broker communication.
        /// </summary>
        StaleControllerEpochCode = 11,

        /// <summary>
        /// If you specify a string larger than configured maximum for offset metadata
        /// </summary>
        OffsetMetadataTooLargeCode = 12,

        /// <summary>
        /// The server disconnected before a response was received.
        /// </summary>
        NetworkException = 13,

        /// <summary>
        /// The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).
        /// </summary>
        OffsetsLoadInProgressCode = 14,

        /// <summary>
        /// The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.
        /// </summary>
        ConsumerCoordinatorNotAvailableCode = 15,

        /// <summary>
        /// The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.
        /// </summary>
        NotCoordinatorForConsumerCode = 16,

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
        UnsupportedVersion = 35
    }

    /// <summary>
    /// Protocol specific constants
    /// </summary>
    public struct ProtocolConstants
    {
        /// <summary>
        ///  The lowest 2 bits contain the compression codec used for the message. The other bits should be set to 0.
        /// </summary>
        public static byte AttributeCodeMask = 0x03;
    }

    /// <summary>
    /// Enumeration which specifies the compression type of messages
    /// </summary>
    public enum MessageCodec
    {
        CodecNone = 0x00,
        CodecGzip = 0x01,
        CodecSnappy = 0x02
    }

    #region Exceptions...

    [Serializable]
    public class FailCrcCheckException : ApplicationException
    {
        public FailCrcCheckException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public FailCrcCheckException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public FailCrcCheckException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    [Serializable]
    public class ResponseTimeoutException : ApplicationException
    {
        public ResponseTimeoutException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public ResponseTimeoutException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public ResponseTimeoutException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    [Serializable]
    public class InvalidPartitionException : ApplicationException
    {
        public InvalidPartitionException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public InvalidPartitionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public InvalidPartitionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    [Serializable]
    public class NoLeaderElectedForPartition : ApplicationException
    {
         public NoLeaderElectedForPartition(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public NoLeaderElectedForPartition(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public NoLeaderElectedForPartition(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }


    [Serializable]
    public class BrokerException : ApplicationException
    {
        public KafkaEndpoint BrokerEndPoint;

        public BrokerException(string message, KafkaEndpoint endPoint, params object[] args)
            : base(string.Format(message, args))
        {
            BrokerEndPoint = endPoint;
        }

        public BrokerException(SerializationInfo info, StreamingContext context) 
            : base(info, context)
        {
            bool hasFetch = info.GetByte("HasBrokerEndPoint") == 1;
            if (hasFetch)
            {
                IPEndPoint ipEndPoint = null;
                Uri uri = null;
                if (info.GetByte("HasEndpoint") == 1)
                {
                    ipEndPoint = new IPEndPoint(info.GetInt64("Address"), info.GetInt32("Port"));
                }

                if (info.GetByte("HasServeUri") == 1)
                {
                    uri = new Uri(info.GetString("ServeUri"));
                }

                BrokerEndPoint = new KafkaEndpoint
                {
                    Endpoint = ipEndPoint,
                    ServeUri = uri

                };
            }
        }

        public BrokerException(string message, KafkaEndpoint endPoint, Exception innerException)
            : base(message, innerException)
        {
            BrokerEndPoint = endPoint;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            bool hasValue = BrokerEndPoint != null;
            info.AddValue("HasBrokerEndPoint", (byte) (hasValue ? 1 : 0));

            if (hasValue)
            {
                info.AddValue("HasEndpoint", (byte) (BrokerEndPoint.Endpoint != null ? 1 : 0));
                info.AddValue("HasServeUri", (byte) (BrokerEndPoint.ServeUri != null ? 1 : 0));

                if (BrokerEndPoint.Endpoint != null)
                {
                    info.AddValue("Address", BrokerEndPoint.Endpoint.Address.Address);
                    info.AddValue("Port", BrokerEndPoint.Endpoint.Port);
                }

                if (BrokerEndPoint.ServeUri != null)
                    info.AddValue("ServeUri", BrokerEndPoint.ServeUri.ToString());
            }
        }
    }

    [Serializable]
    public class BrokerConnectionException : BrokerException
    {
        public BrokerConnectionException(string message, KafkaEndpoint endPoint, params object[] args)
            : base(string.Format(message, args), endPoint)
        {
        }

        public BrokerConnectionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public BrokerConnectionException(string message, KafkaEndpoint endPoint, Exception innerException)
            : base(message, endPoint, innerException)
        {
            BrokerEndPoint = endPoint;
        }
    }

    [Serializable]
    public class ServerUnreachableException : ApplicationException
    {
        public ServerUnreachableException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public ServerUnreachableException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public ServerUnreachableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    [Serializable]
    public class InvalidTopicMetadataException : ApplicationException
    {
        public ErrorResponseCode ErrorResponseCode { get; private set; }

        public InvalidTopicMetadataException(ErrorResponseCode code, string message, params object[] args)
            : base(string.Format(message, args))
        {
            ErrorResponseCode = code;
        }

        public InvalidTopicMetadataException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public InvalidTopicMetadataException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ErrorResponseCode = (ErrorResponseCode)info.GetInt16("ErrorResponseCode");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("ErrorResponseCode", (short)ErrorResponseCode);
        }
    }

    [Serializable]
    public class InvalidTopicNotExistsInCache : Exception
    {
        public InvalidTopicNotExistsInCache(string info)
            : base(info)
        {
        }

        public InvalidTopicNotExistsInCache(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public InvalidTopicNotExistsInCache(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    [Serializable]
    public class LeaderNotFoundException : ApplicationException
    {
        public LeaderNotFoundException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public LeaderNotFoundException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public LeaderNotFoundException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    [Serializable]
    public class UnresolvedHostnameException : ApplicationException
    {
        public UnresolvedHostnameException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public UnresolvedHostnameException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public UnresolvedHostnameException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    [Serializable]
    public class InvalidMetadataException : ApplicationException
    {
        public int ErrorCode { get; set; }

        public InvalidMetadataException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public InvalidMetadataException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ErrorCode = info.GetInt32("ErrorCode");
        }

        public InvalidMetadataException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("ErrorCode", ErrorCode);
        }
    }

    [Serializable]
    public class OffsetOutOfRangeException : ApplicationException
    {
        public Fetch FetchRequest { get; set; }

        public OffsetOutOfRangeException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public OffsetOutOfRangeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public OffsetOutOfRangeException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            bool hasFetch = info.GetByte("HasFetch") == 1;
            if (hasFetch)
            {
                FetchRequest = new Fetch
                {
                    MaxBytes = info.GetInt32("MaxBytes"),
                    Offset = info.GetInt64("Offset"),
                    PartitionId = info.GetInt32("PartitionId"),
                    Topic = info.GetString("Topic")
                };
            }
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            bool hasValue = FetchRequest != null;
            info.AddValue("HasFetch", (byte)(hasValue ? 1 : 0));
            if (hasValue)
            {
                info.AddValue("MaxBytes", FetchRequest.MaxBytes);
                info.AddValue("Offset", FetchRequest.Offset);
                info.AddValue("PartitionId", FetchRequest.PartitionId);
                info.AddValue("Topic", FetchRequest.Topic);
            }
        }
    }

    [Serializable]
    public class BufferUnderRunException : ApplicationException
    {
        public int MessageHeaderSize { get; set; }
        public int RequiredBufferSize { get; set; }

        public BufferUnderRunException(int messageHeaderSize, int requiredBufferSize)
            : base("The size of the message from Kafka exceeds the provide buffer size.")
        {
            MessageHeaderSize = messageHeaderSize;
            RequiredBufferSize = requiredBufferSize;
        }

        public BufferUnderRunException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public BufferUnderRunException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            MessageHeaderSize = info.GetInt32("MessageHeaderSize");
            RequiredBufferSize = info.GetInt32("RequiredBufferSize");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("MessageHeaderSize", MessageHeaderSize);
            info.AddValue("RequiredBufferSize", RequiredBufferSize);
        }
    }

    [Serializable]
    public class KafkaApplicationException : ApplicationException
    {
        public int ErrorCode { get; set; }

        public KafkaApplicationException(string message, params object[] args)
            : base(string.Format(message, args))
        {
        }

        public KafkaApplicationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public KafkaApplicationException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ErrorCode = info.GetInt32("ErrorCode");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("ErrorCode", ErrorCode);
        }
    }

    #endregion Exceptions...
}