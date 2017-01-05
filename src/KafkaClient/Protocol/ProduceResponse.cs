using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// ProduceResponse => [Response] *ThrottleTime
    ///  *ThrottleTime is only version 1 (0.9.0) and above
    /// 
    ///  Response => TopicName [PartitionResponse]
    ///   TopicName => string  -- The topic this response entry corresponds to.
    /// 
    ///   PartitionResponse => Partition ErrorCode Offset *Timestamp
    ///    *Timestamp is only version 2 (0.10.0) and above
    ///    Partition => int32  -- The partition this response entry corresponds to.
    ///    ErrorCode => int16  -- The error from this partition, if any. Errors are given on a per-partition basis because a given partition may be 
    ///                             unavailable or maintained on a different host, while others may have successfully accepted the produce request.
    ///    Offset => int64     -- The offset assigned to the first message in the message set appended to this partition.
    ///    Timestamp => int64  -- If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
    ///                           All the messages in the message set have the same timestamp.
    ///                           If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
    ///                           produce request has been accepted by the broker if there is no error code returned.
    ///                          Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
    ///  ThrottleTime => int32 -- Duration in milliseconds for which the request was throttled due to quota violation. 
    ///                           (Zero if the request did not violate any quota).
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
    /// </summary>
    public class ProduceResponse : IResponse, IEquatable<ProduceResponse>
    {
        public ProduceResponse(Topic topic, TimeSpan? throttleTime = null)
            : this (new []{ topic }, throttleTime)
        {
        }

        public ProduceResponse(IEnumerable<Topic> topics = null, TimeSpan? throttleTime = null)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
            ThrottleTime = throttleTime;
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<Topic> Topics { get; }

        /// <summary>
        /// Duration in milliseconds for which the request was throttled due to quota violation. 
        /// (Zero if the request did not violate any quota). Only version 1 (0.9.0) and above.
        /// </summary>
        public TimeSpan? ThrottleTime { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ProduceResponse);
        }

        /// <inheritdoc />
        public bool Equals(ProduceResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics) 
                && (int?)ThrottleTime?.TotalMilliseconds == (int?)other.ThrottleTime?.TotalMilliseconds;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Topics?.GetHashCode() ?? 0;
                hashCode = (hashCode*397) ^ ThrottleTime.GetHashCode();
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ProduceResponse left, ProduceResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ProduceResponse left, ProduceResponse right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public Topic(string topic, int partitionId, ErrorResponseCode errorCode, long offset, DateTimeOffset? timestamp = null)
                : base(topic, partitionId, errorCode)
            {
                Offset = offset;
                Timestamp = timestamp.HasValue && timestamp.Value.ToUnixTimeMilliseconds() >= 0 ? timestamp : null;
            }

            /// <summary>
            /// The offset number to commit as completed.
            /// </summary>
            public long Offset { get; }

            /// <summary>
            /// If LogAppendTime is used for the topic, this is the timestamp assigned by the broker to the message set. 
            /// All the messages in the message set have the same timestamp.
            /// If CreateTime is used, this field is always -1. The producer can assume the timestamp of the messages in the 
            /// produce request has been accepted by the broker if there is no error code returned.
            /// </summary>
            public DateTimeOffset? Timestamp { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) 
                    && Offset == other.Offset 
                    && Timestamp.Equals(other.Timestamp);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Offset.GetHashCode();
                    hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
                    return hashCode;
                }
            }

            public static bool operator ==(Topic left, Topic right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(Topic left, Topic right)
            {
                return !Equals(left, right);
            }

            #endregion

        }
    }
}