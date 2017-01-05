using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// FetchRequest => ReplicaId MaxWaitTime MinBytes *MaxBytes [TopicData]
    ///  *MaxBytes is only version 3 (0.10.1) and above
    ///  ReplicaId => int32   -- The replica id indicates the node id of the replica initiating this request. Normal client consumers should always 
    ///                          specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted 
    ///                          to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
    ///  MaxWaitTime => int32 -- The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available 
    ///                          at the time the request is issued.
    ///  MinBytes => int32    -- This is the minimum number of bytes of messages that must be available to give a response. If the client sets this 
    ///                          to 0 the server will always respond immediately, however if there is no new data since their last request they will 
    ///                          just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has 
    ///                          at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the 
    ///                          consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. 
    ///                          setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 
    ///                          64k of data before responding).
    ///  MaxBytes => int32    -- Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, if the first message in the 
    ///                          first non-empty partition of the fetch is larger than this value, the message will still be returned to ensure that 
    ///                          progress can be made.
    /// 
    ///  TopicData => TopicName [PartitionData]
    ///   TopicName => string   -- The name of the topic.
    /// 
    ///   PartitionData => Partition FetchOffset MaxBytes
    ///    Partition => int32   -- The id of the partition the fetch is for.
    ///    FetchOffset => int64 -- The offset to begin this fetch from.
    ///    MaxBytes => int32    -- The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
    /// 
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
    /// </summary>
    public class FetchRequest : Request, IRequest<FetchResponse>, IEquatable<FetchRequest>
    {
        public FetchRequest(Topic topic, TimeSpan? maxWaitTime = null, int? minBytes = null, int? maxBytes = null) 
            : this (new []{ topic }, maxWaitTime, minBytes, maxBytes)
        {
        }

        public FetchRequest(IEnumerable<Topic> fetches = null, TimeSpan? maxWaitTime = null, int? minBytes = null, int? maxBytes = null) 
            : base(ApiKeyRequestType.Fetch)
        {
            MaxWaitTime = maxWaitTime ?? TimeSpan.FromMilliseconds(DefaultMaxBlockingWaitTime);
            MinBytes = minBytes.GetValueOrDefault(DefaultMinBlockingByteBufferSize);
            MaxBytes = maxBytes.GetValueOrDefault();
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(fetches);
        }

        internal const int DefaultMinBlockingByteBufferSize = 4096;
        internal const int DefaultBufferSize = DefaultMinBlockingByteBufferSize * 8;
        internal const int DefaultMaxBlockingWaitTime = 5000;

        /// <summary>
        /// The max wait time is the maximum amount of time to block waiting if insufficient data is available at the time the request is issued.
        /// </summary>
        public TimeSpan MaxWaitTime { get; }

        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response.
        /// If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets.
        /// If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs.
        /// By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data
        /// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        public int MinBytes { get; }

        /// <summary>
        /// Maximum bytes to accumulate in the response. Note that this is not an absolute maximum, if the first message in the first non-empty partition of the fetch is larger than 
        /// this value, the message will still be returned to ensure that progress can be made.
        /// </summary>
        public int MaxBytes { get; }

        public IImmutableList<Topic> Topics { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as FetchRequest);
        }

        /// <inheritdoc />
        public bool Equals(FetchRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return MaxWaitTime.Equals(other.MaxWaitTime) 
                && MinBytes == other.MinBytes 
                && MaxBytes == other.MaxBytes 
                && Topics.HasEqualElementsInOrder(other.Topics);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = MaxWaitTime.GetHashCode();
                hashCode = (hashCode*397) ^ MinBytes;
                hashCode = (hashCode*397) ^ MaxBytes;
                hashCode = (hashCode*397) ^ (Topics?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(FetchRequest left, FetchRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(FetchRequest left, FetchRequest right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class Topic : TopicPartition, IEquatable<Topic>
        {
            public Topic(string topicName, int partitionId, long offset, int? maxBytes = null)
                : base(topicName, partitionId)
            {
                Offset = offset;
                MaxBytes = maxBytes.GetValueOrDefault(FetchRequest.DefaultMinBlockingByteBufferSize * 8);
            }

            /// <summary>
            /// The offset to begin this fetch from.
            /// </summary>
            public long Offset { get; }

            /// <summary>
            /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
            /// </summary>
            public int MaxBytes { get; }

            #region Equality

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Equals((TopicPartition) other) 
                    && Offset == other.Offset 
                    && MaxBytes == other.MaxBytes;
            }

            public override int GetHashCode()
            {
                unchecked {
                    int hashCode = base.GetHashCode();
                    hashCode = (hashCode*397) ^ Offset.GetHashCode();
                    hashCode = (hashCode*397) ^ MaxBytes;
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