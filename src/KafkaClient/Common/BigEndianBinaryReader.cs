using System;
using System.IO;
using System.Text;

namespace KafkaClient.Common
{
    /// <summary>
    /// A BinaryReader that is BigEndian aware binary reader.
    /// </summary>
    /// <remarks>
    /// Booleans, bytes and byte arrays will be written directly.
    /// All other values will be converted to a byte array in BigEndian byte order and written.
    /// Characters and Strings will all be encoded in UTF-8 (which is byte order independent).
    /// </remarks>
    /// <remarks>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    ///
    /// The code was modified to provide Kafka specific logic and helper functions.
    /// Specifically where STRINGS are 16bit prefixed and BYTE[] are 32bit prefixed
    /// </remarks>
    public class BigEndianBinaryReader : BinaryReader, IKafkaReader
    {
        private const int KafkaNullSize = -1;

        public BigEndianBinaryReader(Stream stream)
            : base(stream, Encoding.UTF8)
        {
        }

        public BigEndianBinaryReader(byte[] payload, int offset, int count)
            : this(new MemoryStream(payload, offset, count))
        {
        }

        public BigEndianBinaryReader(byte[] payload, int offset) : this(payload, offset, payload.Length - offset)
        {
        }

        public BigEndianBinaryReader(byte[] payload) : this(payload, 0, payload.Length)
        {
        }

        public long Length => BaseStream.Length;
        public long Position { get { return BaseStream.Position; } set { BaseStream.Position = value; } }
        public bool HasData => BaseStream.Position < BaseStream.Length;

        public bool Available(int dataSize)
        {
            return BaseStream.Length - BaseStream.Position >= dataSize;
        }

        public override decimal ReadDecimal()
        {
            var bytes = GetNextBytesNativeEndian(16);

            var ints = new int[4];
            ints[0] = bytes[0] << 0
                | bytes[1] << 8
                | bytes[2] << 16
                | bytes[3] << 24;
            ints[1] = bytes[4] << 0
                | bytes[5] << 8
                | bytes[6] << 16
                | bytes[7] << 24;
            ints[2] = bytes[8] << 0
                | bytes[9] << 8
                | bytes[10] << 16
                | bytes[11] << 24;
            ints[3] = bytes[12] << 0
                | bytes[13] << 8
                | bytes[14] << 16
                | bytes[15] << 24;

            return new decimal(ints);
        }

        public override float ReadSingle()
        {
            return EndianAwareRead(4, BitConverter.ToSingle);
        }

        public override double ReadDouble()
        {
            return EndianAwareRead(8, BitConverter.ToDouble);
        }

        public override short ReadInt16()
        {
            return EndianAwareRead(2, BitConverter.ToInt16);
        }

        public override int ReadInt32()
        {
            return EndianAwareRead(4, BitConverter.ToInt32);
        }

        public override long ReadInt64()
        {
            return EndianAwareRead(8, BitConverter.ToInt64);
        }

        public override ushort ReadUInt16()
        {
            return EndianAwareRead(2, BitConverter.ToUInt16);
        }

        public override uint ReadUInt32()
        {
            return EndianAwareRead(4, BitConverter.ToUInt32);
        }

        public override ulong ReadUInt64()
        {
            return EndianAwareRead(8, BitConverter.ToUInt64);
        }

        public override string ReadString()
        {
            var size = ReadInt16();
            if (size == KafkaNullSize) return null;
            return Encoding.UTF8.GetString(RawRead(size));
        }

        public byte[] ReadBytes()
        {
            var size = ReadInt32();
            if (size == KafkaNullSize) { return null; }
            return RawRead(size);
        }

        public byte[] ReadToEnd()
        {
            var size = (int)(BaseStream.Length - BaseStream.Position);
            var buffer = new byte[size];
            BaseStream.Read(buffer, 0, size);
            return buffer;
        }

        public byte[] CrcHash(int? size = null)
        {
            var currentPosition = BaseStream.Position;
            try {
                if (size.HasValue) {
                    return Crc32Provider.ComputeHash(RawRead(size.Value));
                } else {
                    BaseStream.Position = 0;
                    return Crc32Provider.ComputeHash(ReadToEnd());
                }
            } finally {
                BaseStream.Position = currentPosition;
            }
        }

        public uint Crc()
        {
            var currentPosition = BaseStream.Position;
            try {
                BaseStream.Position = 0;
                return Crc32Provider.Compute(ReadToEnd());
            } finally {
                BaseStream.Position = currentPosition;
            }
        }

        public byte[] RawRead(int size)
        {
            if (size <= 0) { return new byte[0]; }

            var buffer = new byte[size];
            Read(buffer, 0, size);
            return buffer;
        }

        private T EndianAwareRead<T>(int size, Func<byte[], int, T> converter) where T : struct
        {
            if (size < 0) throw new ArgumentOutOfRangeException(nameof(size), size, "Must be >= 0");
            if (converter == null) throw new ArgumentNullException(nameof(converter));

            var bytes = GetNextBytesNativeEndian(size);
            return converter(bytes, 0);
        }

        private byte[] GetNextBytesNativeEndian(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Must be >= 0");

            var bytes = GetNextBytes(count);
            if (BitConverter.IsLittleEndian) {
                Array.Reverse(bytes);
            }
            return bytes;
        }

        private byte[] GetNextBytes(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), count, "Must be >= 0");

            var buffer = new byte[count];
            var bytesRead = BaseStream.Read(buffer, 0, count);

            if (bytesRead != count) throw new EndOfStreamException();

            return buffer;
        }
    }
}