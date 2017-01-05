using System;
using System.IO;
using System.Text;

namespace KafkaClient.Common
{
    /// <summary>
    /// A BinaryWriter that stores values in BigEndian format.
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
    /// The code was modified to implement Kafka specific byte handling.
    /// Specifically where STRINGS are 16bit prefixed and BYTE[] are 32bit prefixed
    /// </remarks>
    public class BigEndianBinaryWriter : BinaryWriter
    {
        public BigEndianBinaryWriter(Stream stream)
            : base(stream, Encoding.UTF8)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
        }

        public BigEndianBinaryWriter(Stream stream, bool leaveOpen)
            : base(stream, Encoding.UTF8, leaveOpen)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
        }

        public override void Write(decimal value)
        {
            var ints = decimal.GetBits(value);
            if (ints == null) throw new ArgumentNullException(nameof(value));
            if (ints.Length != 4) throw new ArgumentOutOfRangeException(nameof(value), value, "Must evaluate to 4 ints");

            if (BitConverter.IsLittleEndian)
                Array.Reverse(ints);

            for (var i = 0; i < 4; ++i)
            {
                var bytes = BitConverter.GetBytes(ints[i]);
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(bytes);

                base.Write(bytes);
            }
        }

        public override void Write(float value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(double value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(short value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(int value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(long value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(ushort value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(uint value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public override void Write(ulong value)
        {
            var bytes = BitConverter.GetBytes(value);
            WriteBigEndian(bytes);
        }

        public void Write(byte[] value, bool includePrefix)
        {
            if (value == null) {
                if (includePrefix) {
                    Write(-1);
                }
                return;
            }

            if (includePrefix) {
                Write(value.Length);
            }
            base.Write(value);
        }

        public override void Write(string value)
        {
            if (value == null) {
                Write((short)-1);
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(value); 
            Write((short)bytes.Length);
            base.Write(bytes);
        }

        private void WriteBigEndian(byte[] bytes)
        {
            if (bytes == null) throw new ArgumentNullException(nameof(bytes));

            base.Write(bytes.ToBigEndian());
        }
    }
}