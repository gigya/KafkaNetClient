using System.Diagnostics.CodeAnalysis;

namespace KafkaClient.Protocol
{
    [SuppressMessage("ReSharper", "UnusedTypeParameter")]
    public interface IRequest<T> : IRequest
        where T : IResponse
    {
    }

    public interface IRequest
    {
        /// <summary>
        /// Indicates this request should wait for a response from the broker
        /// </summary>
        bool ExpectResponse { get; }

        /// <summary>
        /// If there is a type on the request, expose it generally so it can be used on the response
        /// </summary>
        string ProtocolType { get; }

        /// <summary>
        /// Enum identifying the specific type of request message being represented.
        /// </summary>
        ApiKeyRequestType ApiKey { get; }
    }
}