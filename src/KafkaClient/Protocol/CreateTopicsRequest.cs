using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// CreateTopics Request => [create_topic_requests] timeout 
    ///  create_topic_requests => topic num_partitions replication_factor [replica_assignment] [configs] 
    ///    topic => STRING
    ///    num_partitions => INT32
    ///    replication_factor => INT16
    ///    replica_assignment => partition_id [replica] 
    ///      partition_id => INT32
    ///      replica => INT32
    ///    configs => config_key config_value 
    ///      config_key => STRING
    ///      config_value => STRING
    ///  timeout => INT32
    /// </summary>
    public class CreateTopicsRequest : Request, IRequest<CreateTopicsResponse>, IEquatable<CreateTopicsRequest>
    {
        public CreateTopicsRequest(params Topic[] topics)
            : this(topics, null)
        {
        }

        public CreateTopicsRequest(IEnumerable<Topic> topics = null, TimeSpan? timeout = null)
            : base(ApiKeyRequestType.CreateTopics)
        {
            Topics = ImmutableList<Topic>.Empty.AddNotNullRange(topics);
            Timeout = timeout ?? TimeSpan.Zero;
        }

        public IImmutableList<Topic> Topics { get; }

        /// <summary>
        /// The time in ms to wait for a topic to be completely created on the controller node. Values &lt;= 0 will trigger 
        /// topic creation and return immediately
        /// </summary>
        public TimeSpan Timeout { get; }

        public override bool Equals(object obj)
        {
            return Equals(obj as CreateTopicsRequest);
        }

        public bool Equals(CreateTopicsRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Topics.HasEqualElementsInOrder(other.Topics)
                && Timeout.Equals(other.Timeout);
        }

        public override int GetHashCode()
        {
            unchecked {
                return ((Topics?.GetHashCode() ?? 0) * 397) ^ Timeout.GetHashCode();
            }
        }

        public static bool operator ==(CreateTopicsRequest left, CreateTopicsRequest right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(CreateTopicsRequest left, CreateTopicsRequest right)
        {
            return !Equals(left, right);
        }

        public class Topic : IEquatable<Topic>
        {
            public Topic(string topicName, int numberOfPartitions, short replicationFactor, IEnumerable<KeyValuePair<string, string>> configs = null)
                : this (topicName, configs)
            {
                NumberOfPartitions = numberOfPartitions;
                ReplicationFactor = replicationFactor;
                ReplicaAssignments = ImmutableList<ReplicaAssignment>.Empty;
            }

            public Topic(string topicName, IEnumerable<ReplicaAssignment> replicaAssignments, IEnumerable<KeyValuePair<string, string>> configs = null)
                : this (topicName, configs)
            {
                NumberOfPartitions = -1;
                ReplicationFactor = -1;
                ReplicaAssignments = ImmutableList<ReplicaAssignment>.Empty.AddNotNullRange(replicaAssignments);
            }

            private Topic(string topicName, IEnumerable<KeyValuePair<string, string>> configs)
            {
                TopicName = topicName;
                Configs = ImmutableDictionary<string, string>.Empty.AddNotNullRange(configs);
            }

            /// <summary>
            /// Name for newly created topic.
            /// </summary>
            public string TopicName { get; }

            /// <summary>
            /// Number of partitions to be created. -1 indicates unset.
            /// </summary>
            public int NumberOfPartitions { get; }

            /// <summary>
            /// Replication factor for the topic. -1 indicates unset.
            /// </summary>
            public short ReplicationFactor { get; }

            /// <summary>
            /// 	Replica assignment among kafka brokers for this topic partitions. 
            /// If this is set num_partitions and replication_factor must be unset.
            /// </summary>
            public IImmutableList<ReplicaAssignment> ReplicaAssignments { get; }

            /// <summary>
            /// Topic level configuration for topic to be set.
            /// </summary>
            public IImmutableDictionary<string, string> Configs { get; }

            public override bool Equals(object obj)
            {
                return Equals(obj as Topic);
            }

            public bool Equals(Topic other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return String.Equals(TopicName, other.TopicName) 
                    && NumberOfPartitions == other.NumberOfPartitions 
                    && ReplicationFactor == other.ReplicationFactor 
                    && ReplicaAssignments.HasEqualElementsInOrder(other.ReplicaAssignments) 
                    && Configs.HasEqualElementsInOrder(other.Configs);
            }

            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = TopicName?.GetHashCode() ?? 0;
                    hashCode = (hashCode * 397) ^ NumberOfPartitions;
                    hashCode = (hashCode * 397) ^ ReplicationFactor.GetHashCode();
                    hashCode = (hashCode * 397) ^ (ReplicaAssignments?.GetHashCode() ?? 0);
                    hashCode = (hashCode * 397) ^ (Configs?.GetHashCode() ?? 0);
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

        public class ReplicaAssignment : IEquatable<ReplicaAssignment>
        {
            public ReplicaAssignment(int partitionId, IEnumerable<int> replicas = null)
            {
                PartitionId = partitionId;
                Replicas = ImmutableList<int>.Empty.AddNotNullRange(replicas);
            }

            public int PartitionId { get; }

            /// <summary>
            /// The set of all nodes that should host this partition. 
            /// The first replica in the list is the preferred leader.
            /// </summary>
            public IImmutableList<int> Replicas { get; }

            public override bool Equals(object obj)
            {
                return Equals(obj as ReplicaAssignment);
            }

            public bool Equals(ReplicaAssignment other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return PartitionId == other.PartitionId 
                    && Replicas.HasEqualElementsInOrder(other.Replicas);
            }

            public override int GetHashCode()
            {
                unchecked {
                    return (PartitionId * 397) ^ (Replicas?.GetHashCode() ?? 0);
                }
            }

            public static bool operator ==(ReplicaAssignment left, ReplicaAssignment right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(ReplicaAssignment left, ReplicaAssignment right)
            {
                return !Equals(left, right);
            }
        }
    }
}