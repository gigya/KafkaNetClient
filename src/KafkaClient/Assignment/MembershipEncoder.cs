using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    public abstract class MembershipEncoder<TMetadata, TAssignment> : IMembershipEncoder
        where TMetadata : IMemberMetadata
        where TAssignment : IMemberAssignment
    {
        protected MembershipEncoder(string protocolType, IEnumerable<IMembershipAssignor> assignors = null)
        {
            ProtocolType = protocolType;
            _assignors = assignors != null 
                ? assignors.ToImmutableDictionary(a => a.AssignmentStrategy)
                : ImmutableDictionary<string, IMembershipAssignor>.Empty;
        }

        private readonly IImmutableDictionary<string, IMembershipAssignor> _assignors;

        /// <inheritdoc />
        public string ProtocolType { get; }

        /// <inheritdoc />
        public void EncodeMetadata(IKafkaWriter writer, IMemberMetadata value)
        {
            using (writer.MarkForLength()) {
                EncodeMetadata(writer, (TMetadata) value);
            }
        }

        /// <inheritdoc />
        public void EncodeAssignment(IKafkaWriter writer, IMemberAssignment value)
        {
            using (writer.MarkForLength()) {
                EncodeAssignment(writer, (TAssignment) value);
            }
        }

        /// <inheritdoc />
        public IMemberMetadata DecodeMetadata(string assignmentStrategy, IKafkaReader reader)
        {
            var expectedLength = reader.ReadInt32();
            if (!reader.Available(expectedLength)) throw new BufferUnderRunException($"{ProtocolType} Metadata size of {expectedLength} is not fully available.");
            
            return DecodeMetadata(assignmentStrategy, reader, expectedLength);
        }

        /// <inheritdoc />
        public IMemberAssignment DecodeAssignment(IKafkaReader reader)
        {
            var expectedLength = reader.ReadInt32();
            if (!reader.Available(expectedLength)) throw new BufferUnderRunException($"{ProtocolType} Assignment size of {expectedLength} is not fully available.");
            
            return DecodeAssignment(reader, expectedLength);
        }

        protected abstract void EncodeMetadata(IKafkaWriter writer, TMetadata value);
        protected abstract void EncodeAssignment(IKafkaWriter writer, TAssignment value);
        protected abstract TMetadata DecodeMetadata(string assignmentStrategy, IKafkaReader reader, int expectedLength);
        protected abstract TAssignment DecodeAssignment(IKafkaReader reader, int expectedLength);

        public IMembershipAssignor GetAssignor(string strategy)
        {
            IMembershipAssignor assignor;
            if (!_assignors.TryGetValue(strategy ?? "", out assignor)) throw new ArgumentOutOfRangeException(nameof(strategy), $"Unknown strategy {strategy} for ProtocolType {ProtocolType}");
            return assignor;
        }
    }
}