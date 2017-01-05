using System;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Tests
{
    public class ByteTypeMetadata : IMemberMetadata, IEquatable<ByteTypeMetadata>
    {
        private static readonly byte[] Empty = {};

        public ByteTypeMetadata(string assignmentStrategy, byte[] bytes)
        {
            Bytes = bytes ?? Empty;
            AssignmentStrategy = assignmentStrategy;
        }

        public string AssignmentStrategy { get; }

        public byte[] Bytes { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as ByteTypeMetadata);
        }

        public override int GetHashCode()
        {
            unchecked {
                return ((AssignmentStrategy?.GetHashCode() ?? 0) * 397) ^ (Bytes?.GetHashCode() ?? 0);
            }
        }

        public static bool operator ==(ByteTypeMetadata left, ByteTypeMetadata right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(ByteTypeMetadata left, ByteTypeMetadata right)
        {
            return !Equals(left, right);
        }

        /// <inheritdoc />
        public bool Equals(ByteTypeMetadata other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(AssignmentStrategy, other.AssignmentStrategy) 
                && Bytes.HasEqualElementsInOrder(other.Bytes);
        }
    }
}