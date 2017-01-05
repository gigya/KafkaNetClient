using System;

namespace KafkaClient.Protocol
{
    public class TopicResponse : TopicPartition, IEquatable<TopicResponse>
    {
        public TopicResponse(string topicName, int partitionId, ErrorResponseCode errorCode)
            : base(topicName, partitionId)
        {
            ErrorCode = errorCode;
        }

        /// <summary>
        /// Error response code.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as TopicResponse);
        }

        public bool Equals(TopicResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other)
                && ErrorCode == other.ErrorCode;
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (int) ErrorCode;
                return hashCode;
            }
        }

        public static bool operator ==(TopicResponse left, TopicResponse right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TopicResponse left, TopicResponse right)
        {
            return !Equals(left, right);
        }

        #endregion

        public override string ToString() => $"{base.ToString()} ErrorCode {ErrorCode}";
    }
}