using System;

namespace KafkaClient.Connections
{
    /// <summary>
    /// An exception cause by a failure in the connection to Kafka
    /// </summary>
    public class ConnectionException : KafkaException
    {
        public ConnectionException(Endpoint endpoint)
            : base($"Lost connection to {endpoint}")
        {
            Endpoint = endpoint;
        }

        public ConnectionException(Endpoint endpoint, Exception innerException)
            : base($"Lost connection to {endpoint}", innerException)
        {
            Endpoint = endpoint;
        }

        public ConnectionException(string message)
            : base(message)
        {
        }

        public ConnectionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public Endpoint Endpoint { get; set; }
    }
}