using System;

namespace KafkaClient.Protocol
{
    public class BufferUnderRunException : KafkaException
    {
        public BufferUnderRunException(int messageHeaderSize, int requiredBufferSize, long remainingBufferSize)
            : base("The size of the message from Kafka exceeds the provide buffer size.")
        {
            MessageHeaderSize = messageHeaderSize;
            RequiredBufferSize = requiredBufferSize;
            RemainingBufferSize = remainingBufferSize;
        }

        public BufferUnderRunException(string message)
            : base(message)
        {
        }

        public BufferUnderRunException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public int MessageHeaderSize { get; set; }
        public int RequiredBufferSize { get; set; }
        public long RemainingBufferSize { get; set; }
    }
}