using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetRequest => ReplicaId [TopicData]
    ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
    ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
    ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
    /// 
    ///  TopicData => TopicName [PartitionData]
    ///   TopicName => string  -- The name of the topic.
    /// 
    ///   PartitionData => Partition Timestamp *MaxNumberOfOffsets
    ///    *MaxNumberOfOffsets is only version 0 (0.10.0.1)
    ///    Partition => int32   -- The id of the partition the fetch is for.
    ///    Timestamp => int64   -- Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the 
    ///                            latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. Note 
    ///                            that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
    ///    MaxNumberOfOffsets => int32 
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI(AKAListOffset)
    /// A funky Protocol for requesting the starting offset of each segment for the requested partition
    /// </summary>
    public class OffsetRequest : Request, IRequest<OffsetResponse>, IEquatable<OffsetRequest>
    {
        public OffsetRequest(params Topic[] topics)
            : this((IEnumerable<Topic>)topics)
        {
        }

        public OffsetRequest(IEnumerable<Topic> offsets) 
            : base(ApiKeyRequestType.Offset)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(offsets);
        }

        public IImmutableList<Topic> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Topics?.GetHashCode() ?? 0;
        }

        /// <inheritdoc />
        public static bool operator ==(OffsetRequest left, OffsetRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetRequest left, OffsetRequest right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public Topic(string topicName, int partitionId, long timestamp = LatestTime, int maxOffsets = DefaultMaxOffsets) : base(topicName, partitionId)
            {
                Timestamp = timestamp;
                MaxOffsets = maxOffsets;
            }

            /// <summary>
            /// Used to ask for all messages before a certain time (ms). There are two special values.
            /// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset.
            /// Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
            /// </summary>
            public long Timestamp { get; }

            /// <summary>
            /// Only applies to version 0 (Kafka 0.10.1 and below)
            /// </summary>
            public int MaxOffsets { get; }

            public const long LatestTime = -1L;
            public const long EarliestTime = -2L;
            public const int DefaultMaxOffsets = 1;

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
                    && Timestamp == other.Timestamp 
                    && MaxOffsets == other.MaxOffsets;
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
                    hashCode = (hashCode*397) ^ MaxOffsets;
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