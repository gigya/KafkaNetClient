using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace KafkaClient.Common
{
    public class KafkaWriter : IKafkaWriter
    {
        private const int IntegerByteSize = 4;
        private readonly BigEndianBinaryWriter _stream;

        public KafkaWriter()
        {
            _stream = new BigEndianBinaryWriter(new MemoryStream());
            Write(IntegerByteSize); //pre-allocate space for buffer length
        }

        public IKafkaWriter Write(bool value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(byte value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(int value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(short value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(long value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(byte[] values, bool includeLength = true)
        {
            _stream.Write(values, includeLength);
            return this;
        }

        public IKafkaWriter Write(string value)
        {
            _stream.Write(value);
            return this;
        }

        public IKafkaWriter Write(IEnumerable<string> values, bool includeLength = false)
        {
            if (includeLength) {
                var valuesList = values.ToList();
                _stream.Write(valuesList.Count);
                Write(valuesList);
                return this;
            }

            foreach (var item in values) {
                _stream.Write(item);
            }
            return this;
        }

        public byte[] ToBytes()
        {
            WriteLength(0);
            return ToBytes(0);
        }

        public byte[] ToBytesNoLength()
        {
            return ToBytes(IntegerByteSize);
        }

        private byte[] ToBytes(int offset)
        {
            var length = _stream.BaseStream.Length - offset;
            var buffer = new byte[length];
            _stream.BaseStream.Position = offset;
            _stream.BaseStream.Read(buffer, 0, (int)length);
            return buffer;
        }

        private void WriteLength(int offset)
        {
            _stream.BaseStream.Position = offset;
            var length = _stream.BaseStream.Length - (offset + IntegerByteSize); 
            Write((int)length);
        }

        private void WriteCrc(int offset)
        {
            _stream.BaseStream.Position = offset + IntegerByteSize;

            var crc = Crc32Provider.ComputeHash(_stream.BaseStream.ToEnumerable());
            _stream.BaseStream.Position = offset;
            _stream.Write(crc);            
        }

        public IDisposable MarkForLength()
        {
            var markerPosition = (int)_stream.BaseStream.Position;
            Write(IntegerByteSize); //pre-allocate space for marker

            return new Disposable(
                () => {
                    WriteLength(markerPosition);
                    _stream.BaseStream.Seek(0, SeekOrigin.End);
                });
        }

        public IDisposable MarkForCrc()
        {
            var markerPosition = (int)_stream.BaseStream.Position;
            Write(IntegerByteSize); //pre-allocate space for marker

            return new Disposable(
                () => {
                    WriteCrc(markerPosition);
                    _stream.BaseStream.Seek(0, SeekOrigin.End);
                });
        }

        public void Dispose()
        {
            using (_stream) { }
        }
    }
}