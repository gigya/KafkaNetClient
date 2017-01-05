using System;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public class SendMessageConfiguration : ISendMessageConfiguration
    {
        public SendMessageConfiguration(short acks = Defaults.Acks, TimeSpan? ackTimeout = null, MessageCodec codec = MessageCodec.CodecNone)
        {
            AckTimeout = ackTimeout ?? TimeSpan.FromSeconds(Defaults.ServerAckTimeoutSeconds);
            Acks = acks;
            Codec = codec;
        }

        /// <inheritdoc />
        public MessageCodec Codec { get; }

        /// <inheritdoc />
        public short Acks { get; }

        /// <inheritdoc />
        public TimeSpan AckTimeout { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default value for <see cref="SendMessageConfiguration.Acks"/>
            /// </summary>
            public const short Acks = 1;

            /// <summary>
            /// The default value for <see cref="SendMessageConfiguration.AckTimeout"/>
            /// </summary>
            public const int ServerAckTimeoutSeconds = 1;
        }
    }
}