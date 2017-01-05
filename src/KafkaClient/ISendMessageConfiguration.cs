using System;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface ISendMessageConfiguration
    {
        /// <summary>
        /// The codec to apply when sending messages.
        /// </summary>
        MessageCodec Codec { get; }

        /// <summary>
        /// Level of ack required by kafka: 0 none (immediate), 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        short Acks { get; }

        /// <summary>
        /// The time kafka will wait for the requested ack level before returning.
        /// </summary>
        TimeSpan AckTimeout { get; }
    }
}