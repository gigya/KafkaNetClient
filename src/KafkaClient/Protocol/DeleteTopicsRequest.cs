using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteTopics Request => [topics] timeout 
    ///  topics => STRING
    ///  timeout => INT32
    /// </summary>
    public class DeleteTopicsRequest : Request, IRequest<DeleteTopicsResponse>, IEquatable<DeleteTopicsRequest>
    {
        public DeleteTopicsRequest(params string[] topics)
            : this(topics, null)
        {
        }

        public DeleteTopicsRequest(IEnumerable<string> topics, TimeSpan? timeout = null)
            : base(ApiKeyRequestType.DeleteTopics)
        {
            Topics = ImmutableList<string>.Empty.AddNotNullRange(topics);
            Timeout = timeout ?? TimeSpan.Zero;
        }

        public IImmutableList<string> Topics { get; }

        /// <summary>
        /// The time in ms to wait for a topic to be completely deleted on the controller node. 
        /// Values &lt;= 0 will trigger topic deletion and return immediately
        /// </summary>
        public TimeSpan Timeout { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as DeleteTopicsRequest);
        }

        public bool Equals(DeleteTopicsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && Topics.HasEqualElementsInOrder(other.Topics)
                && Timeout.Equals(other.Timeout);
        }

        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Topics?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ Timeout.GetHashCode();
                return hashCode;
            }
        }

        public static bool operator ==(DeleteTopicsRequest left, DeleteTopicsRequest right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(DeleteTopicsRequest left, DeleteTopicsRequest right)
        {
            return !Equals(left, right);
        }
    }
}