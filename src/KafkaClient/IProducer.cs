using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IProducer : IKafkaClient
    {
        /// <summary>
        /// Send messages to the given topic.
        /// </summary>
        /// <param name="messages">The messages to send.</param>
        /// <param name="topicName">The name of the kafka topic to send the messages to.</param>
        /// <param name="partition">The partition to send messages to, or <value>null</value> for any.</param>
        /// <param name="configuration">The configuration for sending the messages (ie acks, ack Timeout and codec)</param>
        /// <param name="cancellationToken">The token for cancellation</param>
        /// <returns>List of ProduceTopic response from each partition sent to or empty list if acks = 0.</returns>
        Task<ImmutableList<ProduceResponse.Topic>> SendMessagesAsync(IEnumerable<Message> messages, string topicName, int? partition, ISendMessageConfiguration configuration, CancellationToken cancellationToken);

        /// <summary>
        /// The configuration for various limits and for send defaults
        /// </summary>
        IProducerConfiguration Configuration { get; }
    }
}