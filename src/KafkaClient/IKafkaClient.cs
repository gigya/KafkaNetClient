using System;

namespace KafkaClient
{
    /// <summary>
    /// Common interface for consumer and producer.
    /// </summary>
    public interface IKafkaClient : IDisposable
    {
        /// <summary>
        /// The broker router used to route requests.
        /// </summary>
        IRouter Router { get; }
    }
}