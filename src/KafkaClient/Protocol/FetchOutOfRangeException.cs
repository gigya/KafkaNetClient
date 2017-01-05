using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request for fetching (FetchRequest, FetchOffset, etc)
    /// </summary>
    public class FetchOutOfRangeException : RequestException
    {
        public FetchOutOfRangeException(FetchRequest.Topic topic, ApiKeyRequestType apiKey, ErrorResponseCode errorCode, string message = null)
            : base(apiKey, errorCode, message)
        {
            Topic = topic;
        }

        public FetchOutOfRangeException(string message)
            : base(message)
        {
        }

        public FetchOutOfRangeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public FetchRequest.Topic Topic { get; }
    }
}