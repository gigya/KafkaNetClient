using System;
using KafkaClient.Connections;

namespace KafkaClient
{
    public class Broker : IEquatable<Broker>
    {
        public Broker(int brokerId, IConnection connection)
        {
            BrokerId = brokerId;
            Connection = connection;
        }

        public IConnection Connection { get; }

        public int BrokerId { get; }

        public override string ToString() => $"{Connection.Endpoint.ServerUri} ({BrokerId})";

        public override bool Equals(object obj)
        {
            return Equals(obj as Broker);
        }

        public bool Equals(Broker other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return BrokerId == other.BrokerId;
        }

        public override int GetHashCode()
        {
            return BrokerId;
        }

        public static bool operator ==(Broker left, Broker right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Broker left, Broker right)
        {
            return !Equals(left, right);
        }
    }
}