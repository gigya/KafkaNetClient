using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
    ///  ConsumerGroup => string -- The consumer group id.
    ///  TopicName => string     -- The topic to commit.
    ///  Partition => int32      -- The partition id.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// Class that represents both the request and the response from a kafka server of requesting a stored offset value
    /// for a given consumer group.  Essentially this part of the api allows a user to save/load a given offset position
    /// under any abritrary name.
    /// </summary>
    public class OffsetFetchRequest : Request, IRequest<OffsetFetchResponse>, IEquatable<OffsetFetchRequest>
    {
        public OffsetFetchRequest(string groupId, params TopicPartition[] topics) 
            : this(groupId, (IEnumerable<TopicPartition>)topics)
        {
        }

        public OffsetFetchRequest(string groupId, IEnumerable<TopicPartition> topics) 
            : base(ApiKeyRequestType.OffsetFetch)
        {
            if (string.IsNullOrEmpty(groupId)) throw new ArgumentNullException(nameof(groupId));

            GroupId = groupId;
            Topics = ImmutableList<TopicPartition>.Empty.AddNotNullRange(topics);
        }

        public string GroupId { get; }

        public IImmutableList<TopicPartition> Topics { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetFetchRequest);
        }

        /// <inheritdoc />
        public bool Equals(OffsetFetchRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(GroupId, other.GroupId) 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((GroupId?.GetHashCode() ?? 0)*397) ^ (Topics?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(OffsetFetchRequest left, OffsetFetchRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetFetchRequest left, OffsetFetchRequest right)
        {
            return !Equals(left, right);
        }
    }
}