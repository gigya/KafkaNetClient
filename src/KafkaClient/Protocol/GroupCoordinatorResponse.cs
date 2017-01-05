using System;
using System.Collections.Immutable;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// GroupCoordinatorResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
    ///  ErrorCode => int16        -- The error code.
    ///  CoordinatorId => int32    -- The broker id.
    ///  CoordinatorHost => string -- The hostname of the broker.
    ///  CoordinatorPort => int32  -- The port on which the broker accepts requests.
    ///
    /// From https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class GroupCoordinatorResponse : Broker, IResponse, IEquatable<GroupCoordinatorResponse>
    {
        public GroupCoordinatorResponse(ErrorResponseCode errorCode, int coordinatorId, string host, int port)
            : base(coordinatorId, host, port)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
        }

        /// <summary>
        /// Error code of exception that occured during the request.  Zero if no error.
        /// </summary>
        public ErrorResponseCode ErrorCode { get; }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupCoordinatorResponse);
        }

        /// <inheritdoc />
        public bool Equals(GroupCoordinatorResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && ErrorCode == other.ErrorCode;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (int) ErrorCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(GroupCoordinatorResponse left, GroupCoordinatorResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(GroupCoordinatorResponse left, GroupCoordinatorResponse right)
        {
            return !Equals(left, right);
        }
    }
}