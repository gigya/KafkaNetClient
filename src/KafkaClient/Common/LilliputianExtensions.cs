using System;
using System.Linq;
using System.Text;

namespace KafkaClient.Common
{
    /// <summary>
    /// Provides Big Endian conversion extensions to required types for the Kafka protocol.
    /// </summary>
    public static class LilliputianExtensions
    {
        public static byte[] ToIntSizedBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            return value.Length.ToBytes()
                         .Concat(value.ToBytes())
                         .ToArray();
        }

        public static string ToUtf8String(this byte[] value)
        {
            if (value == null) return string.Empty;

            return Encoding.UTF8.GetString(value);
        }

        public static byte[] ToBytes(this string value)
        {
            if (string.IsNullOrEmpty(value)) return (-1).ToBytes();

            //UTF8 is array of bytes, no endianness
            return Encoding.UTF8.GetBytes(value);
        }

        public static byte[] ToBytes(this int value)
        {
            return BitConverter.GetBytes(value).ToBigEndian();
        }

        public static int ToInt32(this byte[] value)
        {
            return BitConverter.ToInt32(BitConverter.IsLittleEndian ? value.Reverse().ToArray() : value, 0);
        }

        public static byte[] ToBigEndian(this byte[] bytes)
        {
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            return bytes;
        }
    }
}