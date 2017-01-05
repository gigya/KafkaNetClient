using System.IO;
using KafkaClient.Common;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    /// <summary>
    /// BigEndianBinaryWriter code provided by Zoltu
    /// https://github.com/Zoltu/Zoltu.EndianAwareBinaryReaderWriter
    /// </summary>
    /// <remarks>Modified to work with nunit from xunit.</remarks>
    [TestFixture]
    public class BigEndianBinaryWriterTests
    {
        // validates my assumptions about the default implementation doing the opposite of this implementation
        [Test]
        [TestCase(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(1, new byte[] { 0x01, 0x00, 0x00, 0x00 })]
        [TestCase(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(int.MinValue, new byte[] { 0x00, 0x00, 0x00, 0x80 })]
        [TestCase(int.MaxValue, new byte[] { 0xFF, 0xFF, 0xFF, 0x7F })]
        public void NativeBinaryWriterTests(int number, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase(0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase(-1, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(int.MinValue, new byte[] { 0x80, 0x00, 0x00, 0x00 })]
        [TestCase(int.MaxValue, new byte[] { 0x7F, 0xFF, 0xFF, 0xFF })]
        public void Int32Tests(int number, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase((short)0, new byte[] { 0x00, 0x00 })]
        [TestCase((short)1, new byte[] { 0x00, 0x01 })]
        [TestCase((short)(-1), new byte[] { 0xFF, 0xFF })]
        [TestCase(unchecked((short)0x8000), new byte[] { 0x80, 0x00 })]
        [TestCase((short)0x7FFF, new byte[] { 0x7F, 0xFF })]
        public void Int16Tests(short number, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase((uint)0, new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((uint)1, new byte[] { 0x00, 0x00, 0x00, 0x01 })]
        [TestCase((uint)123456789, new byte[] { 0x07, 0x5B, 0xCD, 0x15 })]
        [TestCase((uint)0xffffffff, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
        public void UInt32Tests(uint number, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase((float)(0), new byte[] { 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((float)(1), new byte[] { 0x3F, 0x80, 0x00, 0x00 })]
        [TestCase((float)(-1), new byte[] { 0xBF, 0x80, 0x00, 0x00 })]
        [TestCase(float.MinValue, new byte[] { 0xFF, 0x7F, 0xFF, 0xFF })]
        [TestCase(float.MaxValue, new byte[] { 0x7F, 0x7F, 0xFF, 0xFF })]
        [TestCase(float.PositiveInfinity, new byte[] { 0x7F, 0x80, 0x00, 0x00 })]
        [TestCase(float.NegativeInfinity, new byte[] { 0xFF, 0x80, 0x00, 0x00 })]
        [TestCase(float.NaN, new byte[] { 0xFF, 0xC0, 0x00, 0x00 })]
        public void SingleTests(float number, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase((double)(0), new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((double)(1), new byte[] { 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase((double)(-1), new byte[] { 0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(double.MinValue, new byte[] { 0xFF, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(double.MaxValue, new byte[] { 0x7F, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF })]
        [TestCase(double.PositiveInfinity, new byte[] { 0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(double.NegativeInfinity, new byte[] { 0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        [TestCase(double.NaN, new byte[] { 0xFF, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 })]
        public void DoubleTests(double number, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(number);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase("0000", new byte[] { 0x00, 0x04, 0x30, 0x30, 0x30, 0x30 })]
        [TestCase("€€€€", new byte[] { 0x00, 0x0C, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        [TestCase("", new byte[] { 0x00, 0x00 })]
        [TestCase(null, new byte[] { 0xFF, 0xFF })]
        public void StringTests(string value, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(value);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase('0', new byte[] { 0x30 })]
        [TestCase((char)'€', new byte[] { 0xE2, 0x82, 0xAC })]
        public void CharTests(char value, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(value);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase(new[] { '0', '0', '0', '0' }, new byte[] { 0x30, 0x30, 0x30, 0x30 })]
        [TestCase(new[] { '€', '€', '€', '€' }, new byte[] { 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC, 0xE2, 0x82, 0xAC })]
        public void CharArrayTests(char[] value, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(value);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }

        [Test]
        [TestCase(new[] { '0', '1', '2', '3' }, 1, 2, new byte[] { 0x31, 0x32 })]
        [TestCase(new[] { '€', '2', '€', '€' }, 1, 2, new byte[] { 0x32, 0xE2, 0x82, 0xAC })]
        public void CharSubArrayTests(char[] value, int index, int count, byte[] expectedBytes)
        {
            // arrange
            var memoryStream = new MemoryStream();
            var binaryWriter = new BigEndianBinaryWriter(memoryStream);

            // act
            binaryWriter.Write(value, index, count);

            // assert
            var actualBytes = memoryStream.ToArray();
            Assert.That(expectedBytes, Is.EqualTo(actualBytes));
        }
    }
}