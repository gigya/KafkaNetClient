using KafkaClient.Connections;

namespace KafkaClient
{
    public class TopicBroker : Broker
    {
        public TopicBroker(string topicName, int partitionId, int brokerId, IConnection connection) 
            : base(brokerId, connection)
        {
            TopicName = topicName;
            PartitionId = partitionId;
        }

        public string TopicName { get; }
        public int PartitionId { get; }

        public override string ToString() => $"{base.ToString()} topic/{TopicName}/partition/{PartitionId}";
    }
}