using System;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// Message represents the data from a single event occurance.
    /// </summary>
    public class Message : IEquatable<Message>
    {
        public Message(byte[] value, byte attribute, long offset = 0, int partitionId = 0, byte version = 0, byte[] key = null, DateTimeOffset? timestamp = null)
        {
            Offset = offset;
            PartitionId = partitionId;
            MessageVersion = version;
            Attribute = (byte)(attribute & AttributeMask);
            Key = key;
            Value = value;
            Timestamp = timestamp;
        }

        /// <summary>
        /// Convenience constructor will encode both the key and message to byte streams.
        /// Most of the time a message will be string based.
        /// </summary>
        /// <param name="key">The key value for the message.  Can be null.</param>
        /// <param name="value">The main content data of this message.</param>
        public Message(string value, string key = null)
        {
            Key = key?.ToBytes();
            Value = value.ToBytes();
        }

        /// <summary>
        /// The log offset of this message as stored by the Kafka server.
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// The partition id this offset is from.
        /// </summary>
        public int PartitionId { get; }

        /// <summary>
        /// This is a version id used to allow backwards compatible evolution of the message binary format.
        /// </summary>
        public byte MessageVersion { get; }

        /// <summary>
        /// Attribute value outside message body used for added codec/compression info.
        /// 
        /// The lowest 3 bits contain the compression codec used for the message.
        /// The fourth lowest bit represents the timestamp type. 0 stands for CreateTime and 1 stands for LogAppendTime. The producer should always set this bit to 0. (since 0.10.0)
        /// All other bits should be set to 0.
        /// </summary>
        public byte Attribute { get; }

        /// <summary>
        ///  The lowest 2 bits contain the compression codec used for the message. The other bits should be set to 0.
        /// </summary>
        public const byte AttributeMask = 0x3;

        /// <summary>
        /// Key value used for routing message to partitions.
        /// </summary>
        public byte[] Key { get; }

        /// <summary>
        /// The message body contents.  Can contain compress message set.
        /// </summary>
        public byte[] Value { get; }

        /// <summary>
        /// This is the timestamp of the message. The timestamp type is indicated in the attributes. Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
        /// </summary>
        public DateTimeOffset? Timestamp { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as Message);
        }

        /// <inheritdoc />
        public bool Equals(Message other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Offset == other.Offset 
                && PartitionId == other.PartitionId 
                && MessageVersion == other.MessageVersion 
                && Attribute == other.Attribute 
                && Key.HasEqualElementsInOrder(other.Key)
                && Value.HasEqualElementsInOrder(other.Value) 
                && Timestamp.ToUnixTimeMilliseconds() == other.Timestamp.ToUnixTimeMilliseconds();
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = Offset.GetHashCode();
                hashCode = (hashCode*397) ^ PartitionId;
                hashCode = (hashCode*397) ^ MessageVersion.GetHashCode();
                hashCode = (hashCode*397) ^ Attribute.GetHashCode();
                hashCode = (hashCode*397) ^ (Key?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (Value?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(Message left, Message right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(Message left, Message right)
        {
            return !Equals(left, right);
        }

        #endregion
    }
}