using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SaslHandshake Request (Version: 0) => mechanism 
    ///   mechanism => STRING
    /// </summary>
    public class SaslHandshakeRequest : Request, IRequest<SaslHandshakeResponse>, IEquatable<SaslHandshakeRequest>
    {
        public SaslHandshakeRequest(string mechanism)
            : base(ApiKeyRequestType.SaslHandshake)
        {
            Mechanism = mechanism;
        }

        /// <summary>
        /// SASL Mechanism chosen by the client.
        /// </summary>
        public string Mechanism { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SaslHandshakeRequest);
        }

        /// <inheritdoc />
        public bool Equals(SaslHandshakeRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && string.Equals(Mechanism, other.Mechanism);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (Mechanism?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(SaslHandshakeRequest left, SaslHandshakeRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(SaslHandshakeRequest left, SaslHandshakeRequest right)
        {
            return !Equals(left, right);
        }
    }
}