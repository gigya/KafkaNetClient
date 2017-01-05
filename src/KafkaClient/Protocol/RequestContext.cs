using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Connections;
using KafkaClient.Telemetry;

namespace KafkaClient.Protocol
{
    public class RequestContext : IRequestContext
    {
        public static string DefaultClientId = "Net";

        public RequestContext(int? correlationId = null, short? version = null, string clientId = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, string protocolType = null, ProduceRequestMessages onProduceRequestMessages = null)
        {
            CorrelationId = correlationId.GetValueOrDefault(1);
            ApiVersion = version;
            ClientId = clientId ?? DefaultClientId;
            Encoders = encoders ?? ImmutableDictionary<string, IMembershipEncoder>.Empty;
            ProtocolType = protocolType;
            OnProduceRequestMessages = onProduceRequestMessages;
        }

        /// <summary>
        /// Descriptive name of the source of the messages sent to kafka
        /// </summary>
        public string ClientId { get; }

        /// <summary>
        /// Value supplied will be passed back in the response by the server unmodified.
        /// It is useful for matching request and response between the client and server.
        /// </summary>
        public int CorrelationId { get; }

        /// <summary>
        /// This is a numeric version number for the api request. It allows the server to 
        /// properly interpret the request as the protocol evolves. Responses will always 
        /// be in the format corresponding to the request version.
        /// </summary>
        public short? ApiVersion { get; }

        /// <inheritdoc />
        public IImmutableDictionary<string, IMembershipEncoder> Encoders { get; }

        /// <inheritdoc />
        public string ProtocolType { get; }

        /// <inheritdoc />
        public ProduceRequestMessages OnProduceRequestMessages { get; }
    }
}