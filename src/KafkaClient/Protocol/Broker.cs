using System;

namespace KafkaClient.Protocol
{
    public class Broker : IEquatable<Broker>
    {
        public Broker(int brokerId, string host, int port, string rack = null)
        {
            BrokerId = brokerId;
            Host = host;
            Port = port;
            Rack = rack;
        }

        public int BrokerId { get; }
        public string Host { get; }
        public int Port { get; }
        public string Rack { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Broker);
        }

        public bool Equals(Broker other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return BrokerId == other.BrokerId 
                   && string.Equals(Host, other.Host) 
                   && Port == other.Port
                   && string.Equals(Rack, other.Rack);
        }

        public override int GetHashCode()
        {
            unchecked {
                var hashCode = BrokerId;
                hashCode = (hashCode*397) ^ (Host?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ Port;
                hashCode = (hashCode*397) ^ (Rack?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public static bool operator ==(Broker left, Broker right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Broker left, Broker right)
        {
            return !Equals(left, right);
        }
                
        #endregion

        public override string ToString() => $"http://{Host}:{Port} Broker:{BrokerId} {Rack}";
    }
}