using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// HeartbeatResponse => ErrorCode
    ///   ErrorCode => int16
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class HeartbeatResponse : IResponse, IEquatable<HeartbeatResponse>
    {
        public HeartbeatResponse(ErrorResponseCode errorCode)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public ErrorResponseCode ErrorCode { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as HeartbeatResponse);
        }

        /// <inheritdoc />
        public bool Equals(HeartbeatResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return (int) ErrorCode;
        }

        /// <inheritdoc />
        public static bool operator ==(HeartbeatResponse left, HeartbeatResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(HeartbeatResponse left, HeartbeatResponse right)
        {
            return !Equals(left, right);
        }
    }
}