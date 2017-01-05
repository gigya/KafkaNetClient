using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    public interface IResponse
    {
        /// <summary>
        /// Any errors from the server
        /// </summary>
        IImmutableList<ErrorResponseCode> Errors { get; }
    }
}