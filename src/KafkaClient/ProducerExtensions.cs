using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public static class ProducerExtensions
    {
        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ImmutableList<ProduceResponse.Topic>> SendMessagesAsync(this IProducer producer, IEnumerable<Message> messages, string topicName, CancellationToken cancellationToken)
        {
            return producer.SendMessagesAsync(messages, topicName, null, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partition">The partition to send messages to</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ImmutableList<ProduceResponse.Topic>> SendMessagesAsync(this IProducer producer, IEnumerable<Message> messages, string topicName, int partition, CancellationToken cancellationToken)
        {
            return producer.SendMessagesAsync(messages, topicName, partition, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendMessageAsync(this IProducer producer, Message message, string topicName, CancellationToken cancellationToken)
        {
            return producer.SendMessageAsync(message, topicName, null, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partition">The partition to send messages to</param>
        /// <param name="cancellationToken"></param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static Task<ProduceResponse.Topic> SendMessageAsync(this IProducer producer, Message message, string topicName, int partition, CancellationToken cancellationToken)
        {
            return producer.SendMessageAsync(message, topicName, partition, null, cancellationToken);
        }

        /// <summary>
        /// Send a message to the given topic.
        /// </summary>
        /// <param name="producer">The message producer</param>
        /// <param name="message">The message to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partition">The partition to send messages to, or <value>null</value> for any.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        public static async Task<ProduceResponse.Topic> SendMessageAsync(this IProducer producer, Message message, string topicName, int? partition, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            var result = await producer.SendMessagesAsync(new[] { message }, topicName, partition, configuration, cancellationToken).ConfigureAwait(false);
            return result.SingleOrDefault();
        }
    }
}