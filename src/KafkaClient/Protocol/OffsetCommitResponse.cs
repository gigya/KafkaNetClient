using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
    ///  TopicName => string -- The name of the topic.
    ///  Partition => int32  -- The id of the partition.
    ///  ErrorCode => int16  -- The error code for the partition, if any.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class OffsetCommitResponse : IResponse, IEquatable<OffsetCommitResponse>
    {
        public OffsetCommitResponse(IEnumerable<TopicResponse> topics = null)
        {
            Topics = ImmutableList<TopicResponse>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<TopicResponse> Topics { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetCommitResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetCommitResponse other)
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
        public static bool operator ==(OffsetCommitResponse left, OffsetCommitResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetCommitResponse left, OffsetCommitResponse right)
        {
            return !Equals(left, right);
        }
    }
}