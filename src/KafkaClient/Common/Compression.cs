using System;
using System.IO;
using System.IO.Compression;

namespace KafkaClient.Common
{
    /// <summary>
    /// Extension methods which allow compression of byte arrays
    /// </summary>
    public static class Compression
    {
        public static byte[] Zip(byte[] bytes)
        {
            using (var destination = new MemoryStream()) {
                using (var gzip = new GZipStream(destination, CompressionLevel.Fastest, false)) {
                    gzip.Write(bytes, 0, bytes.Length);
                    gzip.Flush();
                }
                return destination.ToArray();
            }
        }

        public static Stream Unzip(this Stream source)
        {
            var destination = new MemoryStream();
            destination.Write(BitConverter.GetBytes(0), 0, SizeBytes); // placeholder for size
            using (var gzip = new GZipStream(source, CompressionMode.Decompress, true)) {
                gzip.CopyTo(destination);
                gzip.Flush();
            }
            destination.Position = 0;
            destination.Write(((int)destination.Length - SizeBytes).ToBytes(), 0, SizeBytes); // fill the placeholder
            destination.Position = 0;
            return destination;
        }

        private const int SizeBytes = 4;
    }
}