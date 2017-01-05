using System;

namespace KafkaClient
{
    /// <summary>
    /// Base class for all exceptions related to kafka, to make it easier to handle them en mass
    /// </summary>
    public class KafkaException : Exception
    {
        public KafkaException(string message)
            : base(message)
        {
        }

        public KafkaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

    }
}