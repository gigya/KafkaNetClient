using System;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    public class ConnectionFactory : IConnectionFactory
    {
        /// <inheritdoc />
        public IConnection Create(Endpoint endpoint, IConnectionConfiguration configuration, ILog log = null)
        {
            return new Connection(new TcpSocket(endpoint, configuration, log), configuration, log);
        }

        /// <inheritdoc />
        public Endpoint Resolve(Uri kafkaAddress, ILog log)
        {
            return Endpoint.Resolve(kafkaAddress, log);
        }
    }
}