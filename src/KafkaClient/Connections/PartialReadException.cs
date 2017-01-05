using System;

namespace KafkaClient.Connections
{
    /// <summary>
    /// Only thrown by the <see cref="TcpSocket"/> when a read task fails after partially reading from the stream
    /// </summary>
    public class PartialReadException : Exception
    {
        public PartialReadException(int bytesRead, int readSize, Exception innerException)
            : base($"Read {bytesRead} bytes of {readSize} before failing", innerException)
        {
            BytesRead = bytesRead;
            ReadSize = readSize;
        }

        public int BytesRead { get; set; }
        public int ReadSize { get; set; }
    }
}