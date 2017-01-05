using System;
using System.Collections.Immutable;
using KafkaClient.Assignment;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroupResponse => ErrorCode MemberAssignment
    ///   ErrorCode => int16
    ///   MemberAssignment => bytes
    /// 
    /// see https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    /// </summary>
    public class SyncGroupResponse : IResponse, IEquatable<SyncGroupResponse>
    {
        public SyncGroupResponse(ErrorResponseCode errorCode, IMemberAssignment memberAssignment)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
            MemberAssignment = memberAssignment;
        }

        /// <inheritdoc />
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public ErrorResponseCode ErrorCode { get; }
        public IMemberAssignment MemberAssignment { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SyncGroupResponse);
        }

        /// <inheritdoc />
        public bool Equals(SyncGroupResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode 
                && Equals(MemberAssignment, other.MemberAssignment);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return ((int) ErrorCode*397) ^ (MemberAssignment?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(SyncGroupResponse left, SyncGroupResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(SyncGroupResponse left, SyncGroupResponse right)
        {
            return !Equals(left, right);
        }
    }
}