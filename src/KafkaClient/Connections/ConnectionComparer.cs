using System.Collections.Generic;

namespace KafkaClient.Connections
{
    public class ConnectionComparer : IEqualityComparer<IConnection>
    {
        public static readonly ConnectionComparer Singleton = new ConnectionComparer();

        public bool Equals(IConnection x, IConnection y)
        {
            if (ReferenceEquals(null, x)) return ReferenceEquals(null, y);
            if (ReferenceEquals(null, y)) return false;
            if (ReferenceEquals(x, y)) return true;

            return x.Endpoint == y.Endpoint;
        }

        public int GetHashCode(IConnection obj)
        {
            return obj?.Endpoint.GetHashCode() ?? 0;
        }
    }
}