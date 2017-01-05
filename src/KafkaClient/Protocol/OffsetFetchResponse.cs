using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
    ///  TopicName => string -- The name of the topic.
    ///  Partition => int32  -- The id of the partition.
    ///  Offset => int64     -- The offset, or -1 if none exists.
    ///  Metadata => string  -- The metadata associated with the topic and partition.
    ///  ErrorCode => int16  -- The error code for the partition, if any.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class OffsetFetchResponse : IResponse, IEquatable<OffsetFetchResponse>
    {
        public OffsetFetchResponse(IEnumerable<Topic> topics = null)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Topics.Select(t => t.ErrorCode));
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<Topic> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetFetchResponse);
        }

        /// <inheritdoc />
        public bool Equals(OffsetFetchResponse other)
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
        public static bool operator ==(OffsetFetchResponse left, OffsetFetchResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(OffsetFetchResponse left, OffsetFetchResponse right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class Topic : TopicResponse, IEquatable<Topic>
        {
            public Topic(string topic, int partitionId, ErrorResponseCode errorCode, long offset, string metadata) 
                : base(topic, partitionId, errorCode)
            {
                Offset = offset;
                MetaData = metadata;
            }

            /// <summary>
            /// The offset position saved to the server.
            /// </summary>
            public long Offset { get; }

            /// <summary>
            /// Any arbitrary metadata stored during a CommitRequest.
            /// </summary>
            public string MetaData { get; }

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
                       && string.Equals(MetaData, other.MetaData);
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Offset.GetHashCode();
                    hashCode = (hashCode*397) ^ (MetaData?.GetHashCode() ?? 0);
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

            public override string ToString()
            {
                return $"[OffsetFetchResponse TopicName={TopicName}, PartitionID={PartitionId}, Offset={Offset}, Metadata={MetaData}, ErrorCode={ErrorCode}]";
            }
        }

    }
}