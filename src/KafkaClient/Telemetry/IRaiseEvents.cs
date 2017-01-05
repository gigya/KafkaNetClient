using System;
using KafkaClient.Connections;

namespace KafkaClient.Telemetry
{
    public delegate void ProduceRequestMessages(int messages, int requestBytes, int compressionDiffBytes);
    public delegate void ConnectError(Endpoint endpoint, Exception exception);
    public delegate void Connecting(Endpoint endpoint, int attempt, TimeSpan elapsed);
    public delegate void Writing(Endpoint endpoint, DataPayload payload);
    public delegate void WriteSuccess(Endpoint endpoint, DataPayload payload, TimeSpan elapsed);
    public delegate void WriteError(Endpoint endpoint, DataPayload payload, TimeSpan elapsed, Exception exception);
    public delegate void Reading(Endpoint endpoint, int size);
    public delegate void ReadingChunk(Endpoint endpoint, int size, int read, TimeSpan elapsed);
    public delegate void ReadChunk(Endpoint endpoint, int size, int remaining, int read, TimeSpan elapsed);
    public delegate void Read(Endpoint endpoint, byte[] buffer, TimeSpan elapsed);
    public delegate void ReadError(Endpoint endpoint, int size, TimeSpan elapsed, Exception exception);

    public interface IRaiseEvents
    {
        /// <summary>
        /// Triggered when the tcp socket is disconnected.
        /// </summary>
        ConnectError OnDisconnected { get; }

        /// <summary>
        /// Triggered when the tcp socket is connecting.
        /// </summary>
        Connecting OnConnecting { get; } 

        /// <summary>
        /// Triggered after the tcp socket is successfully connected.
        /// </summary>
        Connecting OnConnected { get; } 

        /// <summary>
        /// Triggered after enqueing async write task for writing to the tcp stream.
        /// </summary>
        Writing OnWriteEnqueued { get; } 

        /// <summary>
        /// Triggered when writing to the tcp stream.
        /// </summary>
        Writing OnWriting { get; } 

        /// <summary>
        /// Triggered after having successfully written to the tcp stream.
        /// </summary>
        WriteSuccess OnWritten { get; } 

        /// <summary>
        /// Triggered after failing to write to the tcp stream.
        /// </summary>
        WriteError OnWriteFailed { get; } 

        /// <summary>
        /// Triggered when starting to read a message's bytes from the tcp stream.
        /// </summary>
        Reading OnReading { get; } 

        /// <summary>
        /// Triggered when reading a chunk of bytes from the tcp stream.
        /// </summary>
        ReadingChunk OnReadingChunk { get; } 

        /// <summary>
        /// Triggered after successfully reading a chunk of bytes from the tcp stream.
        /// </summary>
        ReadChunk OnReadChunk { get; } 

        /// <summary>
        /// Triggered after having successfully read a message's bytes from the tcp stream.
        /// </summary>
        Read OnRead { get; } 

        /// <summary>
        /// Triggered after failing to read from the tcp stream.
        /// </summary>
        ReadError OnReadFailed { get; } 

        /// <summary>
        /// Triggered when encoding ProduceRequest messages.
        /// </summary>
        ProduceRequestMessages OnProduceRequestMessages { get; }        
    }
}