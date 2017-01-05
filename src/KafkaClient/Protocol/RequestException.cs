using System;
using KafkaClient.Connections;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// An exception caused by a Kafka Request
    /// </summary>
    public class RequestException : KafkaException
    {
        public RequestException(ApiKeyRequestType apiKey, ErrorResponseCode errorCode, string message = null)
            : base(message ?? $"Kafka returned {errorCode} for {apiKey} request")
        {
            ApiKey = apiKey;
            ErrorCode = errorCode;
        }

        public RequestException(string message)
            : base(message)
        {
        }

        public RequestException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public ApiKeyRequestType ApiKey { get; }
        public ErrorResponseCode ErrorCode { get; }
        public Endpoint Endpoint { get; set; }
    }
}