using System;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// LeaveGroupRequest => group_id member_id 
    ///   group_id => STRING           -- The group id.
    ///   member_id => STRING          -- The member id assigned by the group coordinator.
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// 
    /// To explicitly leave a group, the client can send a leave group request. This is preferred over letting the session timeout expire since 
    /// it allows the group to rebalance faster, which for the consumer means that less time will elapse before partitions can be reassigned to 
    /// an active member.
    /// </summary>
    public class LeaveGroupRequest : Request, IRequest<LeaveGroupResponse>, IGroupMember, IEquatable<LeaveGroupRequest>
    {
        /// <inheritdoc />
        public LeaveGroupRequest(string groupId, string memberId, bool awaitResponse = true) : base(ApiKeyRequestType.LeaveGroup, awaitResponse)
        {
            GroupId = groupId;
            MemberId = memberId;
        }

        /// <inheritdoc />
        public string GroupId { get; }

        /// <inheritdoc />
        public string MemberId { get; }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as LeaveGroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(LeaveGroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                   && string.Equals(GroupId, other.GroupId) 
                   && string.Equals(MemberId, other.MemberId);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (GroupId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (MemberId?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(LeaveGroupRequest left, LeaveGroupRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(LeaveGroupRequest left, LeaveGroupRequest right)
        {
            return !Equals(left, right);
        }
    }
}