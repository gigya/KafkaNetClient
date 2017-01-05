using KafkaClient.Common;

namespace KafkaClient.Assignment
{
    public interface IMembershipEncoder
    {
        string ProtocolType { get; }

        void EncodeMetadata(IKafkaWriter writer, IMemberMetadata value);
        void EncodeAssignment(IKafkaWriter writer, IMemberAssignment value);

        IMemberMetadata DecodeMetadata(string protocol, IKafkaReader reader);
        IMemberAssignment DecodeAssignment(IKafkaReader reader);

        IMembershipAssignor GetAssignor(string strategy);
    }
}