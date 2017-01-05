using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    public class ConsumerEncoder : MembershipEncoder<ConsumerProtocolMetadata, ConsumerMemberAssignment>
    {
        public const string Protocol = "consumer";

        public ConsumerEncoder(params IMembershipAssignor[] assignors) : this((IEnumerable<IMembershipAssignor>)assignors)
        {
        }

        /// <inheritdoc />
        public ConsumerEncoder(IEnumerable<IMembershipAssignor> assignors) : base(Protocol, assignors ?? ConsumerAssignor.Assignors)
        {
        }

        /// <inheritdoc />
        protected override ConsumerProtocolMetadata DecodeMetadata(string assignmentStrategy, IKafkaReader reader, int expectedLength)
        {
            var version = reader.ReadInt16();
            var topicNames = new string[reader.ReadInt32()];
            for (var t = 0; t < topicNames.Length; t++) {
                topicNames[t] = reader.ReadString();
            }
            var userData = reader.ReadBytes();
            return new ConsumerProtocolMetadata(topicNames, assignmentStrategy, userData, version);
        }

        /// <inheritdoc />
        protected override ConsumerMemberAssignment DecodeAssignment(IKafkaReader reader, int expectedLength)
        {
            var version = reader.ReadInt16();

            var topics = new List<TopicPartition>();
            var topicCount = reader.ReadInt32();
            for (var t = 0; t < topicCount; t++) {
                var topicName = reader.ReadString();

                var partitionCount = reader.ReadInt32();
                for (var p = 0; p < partitionCount; p++) {
                    var partitionId = reader.ReadInt32();
                    topics.Add(new TopicPartition(topicName, partitionId));
                }
            }
            var userData = reader.ReadBytes();
            return new ConsumerMemberAssignment(topics, userData, version);
        }

        /// <inheritdoc />
        protected override void EncodeMetadata(IKafkaWriter writer, ConsumerProtocolMetadata value)
        {
            writer.Write(value.Version)
                  .Write(value.Subscriptions, true)
                  .Write(value.UserData);
        }

        /// <inheritdoc />
        protected override void EncodeAssignment(IKafkaWriter writer, ConsumerMemberAssignment value)
        {
            var topicGroups = value.PartitionAssignments.GroupBy(x => x.TopicName).ToList();

            writer.Write(value.Version)
                  .Write(topicGroups.Count);

            foreach (var topicGroup in topicGroups) {
                var partitions = topicGroup.ToList();
                writer.Write(topicGroup.Key)
                        .Write(partitions.Count);

                foreach (var partition in partitions) {
                    writer.Write(partition.PartitionId);
                }
            }
            writer.Write(value.UserData);
        }
    }
}