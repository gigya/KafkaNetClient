using System;
using KafkaClient.Connections;

namespace KafkaClient.Telemetry
{
    public interface ITrackEvents
    {
        void Disconnected(Endpoint endpoint, Exception exception);
        void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed);
        void Connected(Endpoint endpoint, int attempt, TimeSpan elapsed);
        void WriteEnqueued(Endpoint endpoint, DataPayload payload);
        void Writing(Endpoint endpoint, DataPayload payload);
        void Written(Endpoint endpoint, DataPayload payload, TimeSpan elapsed);
        void WriteFailed(Endpoint endpoint, DataPayload payload, TimeSpan elapsed, Exception exception);
        void Reading(Endpoint endpoint, int size);
        void ReadingChunk(Endpoint endpoint, int size, int read, TimeSpan elapsed);
        void ReadChunk(Endpoint endpoint, int size, int remaining, int read, TimeSpan elapsed);
        void Read(Endpoint endpoint, byte[] buffer, TimeSpan elapsed);
        void ReadFailed(Endpoint endpoint, int size, TimeSpan elapsed, Exception exception);
        void ProduceRequestMessages(int messages, int requestBytes, int compressedBytes);
    }
}