using System.Threading;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    internal class SocketPayloadReadTask : CancellableTask<byte[]>
    {
        public SocketPayloadReadTask(int readSize, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            ReadSize = readSize;
        }

        public int ReadSize { get; }
    }
}