using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    public abstract class TopicsResponse : IResponse, IEquatable<TopicsResponse>
    {
        protected TopicsResponse(IEnumerable<Topic> topics = null)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<Topic> Topics { get; } 
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as TopicsResponse);
        }

        public bool Equals(TopicsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics);
        }

        public override int GetHashCode()
        {
            return Topics?.GetHashCode() ?? 0;
        }

        public static bool operator ==(TopicsResponse left, TopicsResponse right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(TopicsResponse left, TopicsResponse right)
        {
            return !Equals(left, right);
        }

        public class Topic : IEquatable<Topic>
        {
            public Topic(string topicName, ErrorResponseCode errorCode)
            {
                TopicName = topicName;
                ErrorCode = errorCode;
            }

            public string TopicName { get; }
            public ErrorResponseCode ErrorCode { get; }

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(TopicName, other.TopicName) && ErrorCode == other.ErrorCode;
            }

            public override int GetHashCode()
            {
                unchecked {
                    return ((TopicName?.GetHashCode() ?? 0) * 397) ^ (int) ErrorCode;
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