using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class ConsumerExtensions
    {
        public static Task<IImmutableList<Message>> FetchMessagesAsync(this IConsumer consumer, OffsetResponse.Topic offset, int maxCount, CancellationToken cancellationToken)
        {
            return consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset, maxCount, cancellationToken);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, OffsetResponse.Topic offset, int batchSize, Func<IEnumerable<Message>, Task> onMessagesAsync, CancellationToken cancellationToken)
        {
            return consumer.FetchAsync(offset.TopicName, offset.PartitionId, offset.Offset, batchSize, onMessagesAsync, cancellationToken);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, OffsetResponse.Topic offset, int batchSize, Func<Message, Task> onMessageAsync, CancellationToken cancellationToken)
        {
            return consumer.FetchAsync(offset.TopicName, offset.PartitionId, offset.Offset, batchSize, onMessageAsync, cancellationToken);
        }

        public static Task<int> FetchAsync(this IConsumer consumer, string topicName, int partitionId, long offset, int batchSize, Func<Message, Task> onMessageAsync, CancellationToken cancellationToken)
        {
            return consumer.FetchAsync(
                topicName, partitionId, offset, batchSize,
                async messages => {
                    foreach (var message in messages) {
                        await onMessageAsync(message);
                    }
                }, cancellationToken);
        }

        public static async Task<int> FetchAsync(this IConsumer consumer, string topicName, int partitionId, long offset, int batchSize, Func<IEnumerable<Message>, Task> onMessagesAsync, CancellationToken cancellationToken)
        {
            var total = 0;
            while (!cancellationToken.IsCancellationRequested) {
                var fetched = await consumer.FetchMessagesAsync(topicName, partitionId, offset + total, batchSize, cancellationToken);
                await onMessagesAsync(fetched);
                total += fetched.Count;
            }
            return total;
        }

        public static Task<IConsumerGroupMember> JoinConsumerGroupAsync(this IConsumer consumer, string groupId, ConsumerProtocolMetadata metadata, CancellationToken cancellationToken)
        {
            return consumer.JoinConsumerGroupAsync(groupId, ConsumerEncoder.Protocol, new[] { metadata }, cancellationToken);
        }

        public static Task<IConsumerGroupMember> JoinConsumerGroupAsync(this IConsumer consumer, string groupId, string protocolType, IMemberMetadata metadata, CancellationToken cancellationToken)
        {
            return consumer.JoinConsumerGroupAsync(groupId, protocolType, new[] { metadata }, cancellationToken);
        }
    }
}