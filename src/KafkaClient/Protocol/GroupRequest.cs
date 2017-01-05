using System;

namespace KafkaClient.Protocol
{
    public abstract class GroupRequest : Request, IGroupMember, IEquatable<GroupRequest>
    {
        /// <inheritdoc />
        protected GroupRequest(ApiKeyRequestType apiKey, string groupId, string memberId, int groupGenerationId = 0, bool expectResponse = true) : base(apiKey, expectResponse)
        {
            if (string.IsNullOrEmpty(groupId)) throw new ArgumentNullException(nameof(groupId));

            GroupId = groupId;
            MemberId = memberId;
            GroupGenerationId = groupGenerationId;
        }

        /// <inheritdoc />
        public string GroupId { get; }

        /// <inheritdoc />
        public string MemberId { get; }

        /// <summary>
        /// The generation of the group.
        /// 
        /// Upon every completion of the join group phase, the coordinator increments a GenerationId for the group. This is returned as a field in the 
        /// response to each member, and is sent in <see cref="HeartbeatRequest"/> and <see cref="OffsetCommitRequest"/>s. When the coordinator rebalances 
        /// a group, the coordinator will send an error code indicating that the member needs to rejoin. If the member does not rejoin before a rebalance 
        /// completes, then it will have an old generationId, which will cause <see cref="ErrorResponseCode.IllegalGeneration"/> errors when included in 
        /// new requests.
        /// </summary>
        public int GroupGenerationId { get; }


        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as GroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(GroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && string.Equals(GroupId, other.GroupId) 
                && string.Equals(MemberId, other.MemberId) 
                && GroupGenerationId == other.GroupGenerationId;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ (GroupId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (MemberId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ GroupGenerationId;
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(GroupRequest left, GroupRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(GroupRequest left, GroupRequest right)
        {
            return !Equals(left, right);
        }
    }
}