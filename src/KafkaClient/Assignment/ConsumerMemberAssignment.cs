using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    /// <summary>
    ///  The format of the MemberAssignment field for consumer groups is included below:
    /// MemberAssignment => Version PartitionAssignment
    ///   Version => int16
    ///   PartitionAssignment => [Topic [Partition]]
    ///     Topic => string
    ///     Partition => int32
    ///   UserData => bytes
    /// All client implementations using the "consumer" protocol type should support this schema.
    /// 
    /// from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public class ConsumerMemberAssignment : IMemberAssignment, IEquatable<ConsumerMemberAssignment>
    {
        private static readonly byte[] Empty = {};

        public ConsumerMemberAssignment(IEnumerable<TopicPartition> partitionAssignments = null, byte[] userData = null, short version = 0)
        {
            Version = version;
            PartitionAssignments = ImmutableList<TopicPartition>.Empty.AddNotNullRange(partitionAssignments);
            UserData = userData ?? Empty;
        }

        public short Version { get; }
        public IImmutableList<TopicPartition> PartitionAssignments { get; }

        public byte[] UserData { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ConsumerMemberAssignment);
        }

        /// <inheritdoc />
        public bool Equals(ConsumerMemberAssignment other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Version == other.Version 
                && PartitionAssignments.HasEqualElementsInOrder(other.PartitionAssignments)
                && UserData.HasEqualElementsInOrder(other.UserData);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Version.GetHashCode();
                hashCode = (hashCode*397) ^ (PartitionAssignments?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (UserData?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(ConsumerMemberAssignment left, ConsumerMemberAssignment right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(ConsumerMemberAssignment left, ConsumerMemberAssignment right)
        {
            return !Equals(left, right);
        }
    }
}