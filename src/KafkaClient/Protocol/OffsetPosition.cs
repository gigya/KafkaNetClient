using System;

namespace KafkaClient.Protocol
{
    public class OffsetPosition : IEquatable<OffsetPosition>
    {
        public OffsetPosition(int partitionId, long offset)
        {
            PartitionId = partitionId;
            Offset = offset;
        }

        public int PartitionId { get; }

        public long Offset { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as OffsetPosition);
        }

        public bool Equals(OffsetPosition other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return PartitionId == other.PartitionId 
                && Offset == other.Offset;
        }

        public override int GetHashCode()
        {
            unchecked {
                return (PartitionId*397) ^ Offset.GetHashCode();
            }
        }

        public static bool operator ==(OffsetPosition left, OffsetPosition right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(OffsetPosition left, OffsetPosition right)
        {
            return !Equals(left, right);
        }

        #endregion

        public override string ToString() => $"Partition/{PartitionId}, Offset {Offset}";

    }
}