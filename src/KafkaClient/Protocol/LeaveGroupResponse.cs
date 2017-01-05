using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// LeaveGroupResponse => ErrorCode
    ///   ErrorCode => int16
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class LeaveGroupResponse : IResponse, IEquatable<LeaveGroupResponse>
    {
        public LeaveGroupResponse(ErrorResponseCode errorCode)
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
            return Equals(obj as LeaveGroupResponse);
        }

        /// <inheritdoc />
        public bool Equals(LeaveGroupResponse other)
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
        public static bool operator ==(LeaveGroupResponse left, LeaveGroupResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(LeaveGroupResponse left, LeaveGroupResponse right)
        {
            return !Equals(left, right);
        }
    }
}