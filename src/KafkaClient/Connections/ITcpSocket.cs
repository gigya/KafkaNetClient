﻿using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Connections
{
    public interface ITcpSocket : IDisposable
    {
        /// <summary>
        /// The IP endpoint to the server.
        /// </summary>
        Endpoint Endpoint { get; }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
        Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken);

        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="payload">The buffer data to send.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation ith size of written bytes..</returns>
        Task<DataPayload> WriteAsync(DataPayload payload, CancellationToken cancellationToken);
    }
}