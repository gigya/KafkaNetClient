using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetCommitRequest => GroupId *GroupGenerationId *MemberId *RetentionTime [TopicData]
    ///  *GroupGenerationId, MemberId is only version 1 (0.8.2) and above
    ///  *RetentionTime is only version 2 (0.9.0) and above
    ///  GroupId => string                  -- The consumer group id.
    ///  GroupGenerationId => int32         -- The generation of the consumer group.
    ///  MemberId => string                 -- The consumer id assigned by the group coordinator.
    ///  RetentionTime => int64             -- Time period in ms to retain the offset.
    /// 
    ///  TopicData => TopicName [PartitionData]
    ///   TopicName => string               -- The topic to commit.
    /// 
    ///   PartitionData => Partition Offset *TimeStamp Metadata
    ///    *TimeStamp is only version 1 (0.8.2)
    ///    Partition => int32               -- The partition id.
    ///    Offset => int64                  -- message offset to be committed.
    ///    Timestamp => int64               -- Commit timestamp.
    ///    Metadata => string               -- Any associated metadata the client wants to keep
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// Class that represents the api call to commit a specific set of offsets for a given topic.  The offset is saved under the
    /// arbitrary ConsumerGroup name provided by the call.
    /// </summary>
    public class OffsetCommitRequest : GroupRequest, IRequest<OffsetCommitResponse>, IEquatable<OffsetCommitRequest>
    {
        public OffsetCommitRequest(string groupId, IEnumerable<Topic> offsetCommits, string memberId = null, int groupGenerationId = -1, TimeSpan? offsetRetention = null) 
            : base(ApiKeyRequestType.OffsetCommit, groupId, memberId ?? "", groupGenerationId)
        {
            OffsetRetention = offsetRetention;
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(offsetCommits);
        }

        /// <summary>
        /// Time period to retain the offset.
        /// </summary>
        public TimeSpan? OffsetRetention { get; }

        public IImmutableList<Topic> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetCommitRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetCommitRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && OffsetRetention.Equals(other.OffsetRetention) 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ OffsetRetention.GetHashCode();
                hashCode = (hashCode*397) ^ (Topics?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(OffsetCommitRequest left, OffsetCommitRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetCommitRequest left, OffsetCommitRequest right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public Topic(string topicName, int partitionId, long offset, string metadata = null, long? timeStamp = null) 
                : base(topicName, partitionId)
            {
                if (offset < -1L) throw new ArgumentOutOfRangeException(nameof(offset), offset, "value must be >= -1");

                Offset = offset;
                TimeStamp = timeStamp;
                Metadata = metadata;
            }

            /// <summary>
            /// The offset number to commit as completed.
            /// </summary>
            public long Offset { get; }

            /// <summary>
            /// If the time stamp field is set to -1, then the broker sets the time stamp to the receive time before committing the offset.
            /// Only version 1 (0.8.2)
            /// </summary>
            public long? TimeStamp { get; }

            /// <summary>
            /// Descriptive metadata about this commit.
            /// </summary>
            public string Metadata { get; }

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
                    && string.Equals(Metadata, other.Metadata);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Offset.GetHashCode();
                    hashCode = (hashCode*397) ^ (Metadata?.GetHashCode() ?? 0);
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
        }

    }
}