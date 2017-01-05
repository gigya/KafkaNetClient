using System;
using System.Collections.Concurrent;
using System.Linq;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class PartitionSelector : IPartitionSelector
    {
        private readonly ConcurrentDictionary<string, int> _roundRobinTracker = new ConcurrentDictionary<string, int>();

        public MetadataResponse.Partition Select(MetadataResponse.Topic topic, byte[] key)
        {
            if (topic == null) throw new ArgumentNullException(nameof(topic));
            if (topic.Partitions.Count <= 0) throw new CachedMetadataException($"topic/{topic.TopicName} has no partitions.") { TopicName = topic.TopicName };

            long partitionId;
            var partitions = topic.Partitions;
            if (key == null) {
                // use round robin
                var paritionIndex = _roundRobinTracker.AddOrUpdate(topic.TopicName, p => 0, (s, i) => (i + 1) % partitions.Count);
                return partitions[paritionIndex];
            } else {
                // use key hash
                partitionId = Crc32Provider.Compute(key) % partitions.Count;
                var partition = partitions.FirstOrDefault(x => x.PartitionId == partitionId);
                if (partition != null) return partition;
            }

            throw new CachedMetadataException($"Hash function return partition/{partitionId}, but the available partitions are {string.Join(",", partitions.Select(x => x.PartitionId))}") {
                TopicName = topic.TopicName,
                Partition = (int)partitionId
            };
        }
    }
}