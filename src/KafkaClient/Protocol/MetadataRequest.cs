using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// TopicMetadataRequest => [TopicName]
    ///  TopicName => string  -- The topics to produce metadata for. If no topics are specified fetch metadata for all topics.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
    /// </summary>
    public class MetadataRequest : Request, IRequest<MetadataResponse>, IEquatable<MetadataRequest>
    {
        public MetadataRequest(string topic)
            : this (new []{topic})
        {
        }

        public MetadataRequest(IEnumerable<string> topics = null) 
            : base(ApiKeyRequestType.Metadata)
        {
            Topics = ImmutableList<string>.Empty.AddNotNullRange(topics);
        }

        /// <summary>
        /// The list of topics to get metadata for.
        /// </summary>
        public IImmutableList<string> Topics { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as MetadataRequest);
        }

        /// <inheritdoc />
        public bool Equals(MetadataRequest other)
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
        public static bool operator ==(MetadataRequest left, MetadataRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(MetadataRequest left, MetadataRequest right)
        {
            return !Equals(left, right);
        }
    }
}