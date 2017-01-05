using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Tests
{
    public class ByteMembershipEncoder : MembershipEncoder<ByteTypeMetadata, ByteTypeAssignment>
    {
        /// <inheritdoc />
        public ByteMembershipEncoder(string protocolType) : base(protocolType)
        {
        }

        /// <inheritdoc />
        protected override void EncodeMetadata(IKafkaWriter writer, ByteTypeMetadata value)
        {
            writer.Write(value.Bytes);
        }

        /// <inheritdoc />
        protected override void EncodeAssignment(IKafkaWriter writer, ByteTypeAssignment value)
        {
            writer.Write(value.Bytes);
        }

        protected override ByteTypeMetadata DecodeMetadata(string assignmentStrategy, IKafkaReader reader, int expectedLength)
        {
            return new ByteTypeMetadata(assignmentStrategy, reader.ReadBytes());
        }

        protected override ByteTypeAssignment DecodeAssignment(IKafkaReader reader, int expectedLength)
        {
            return new ByteTypeAssignment(reader.ReadBytes());
        }
    }
}