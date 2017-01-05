using System.Threading;
using KafkaClient.Common;

namespace KafkaClient.Connections
{
    internal class SocketPayloadWriteTask : CancellableTask<DataPayload>
    {
        public DataPayload Payload { get; }

        public SocketPayloadWriteTask(DataPayload payload, CancellationToken cancellationToken)
            : base(cancellationToken)
        {
            Payload = payload;
        }
    }
}