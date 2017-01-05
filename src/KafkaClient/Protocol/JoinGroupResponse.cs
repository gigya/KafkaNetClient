using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// JoinGroup Response => error_code generation_id group_protocol leader_id member_id [member] 
    ///   error_code => INT16
    ///   generation_id => INT32   -- The generation of the consumer group.
    ///   group_protocol => STRING -- The group protocol selected by the coordinator (ie AssignmentStrategy)
    ///   leader_id => STRING      -- The leader of the group
    ///   member_id => STRING      -- The consumer id assigned by the group coordinator.
    ///   member => member_id member_metadata 
    ///     member_id => STRING
    ///     member_metadata => BYTES
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages for details
    /// 
    /// After receiving join group requests from all members in the group, the coordinator will select one member to be the group leader 
    /// and a protocol which is supported by all members. The leader will receive the full list of members along with the associated metadata 
    /// for the protocol chosen. Other members, followers, will receive an empty array of members. It is the responsibility of the leader to 
    /// inspect the metadata of each member and assign state using SyncGroup request below.
    /// 
    /// Upon every completion of the join group phase, the coordinator increments a GenerationId for the group. This is returned as a field in 
    /// the response to each member, and is sent in heartbeats and offset commit requests. When the coordinator rebalances a group, the 
    /// coordinator will send an error code indicating that the member needs to rejoin. If the member does not rejoin before a rebalance 
    /// completes, then it will have an old generationId, which will cause ILLEGAL_GENERATION errors when included in new requests.
    /// </summary>
    public class JoinGroupResponse : IResponse, IEquatable<JoinGroupResponse>
    {
        public JoinGroupResponse(ErrorResponseCode errorCode, int generationId, string groupProtocol, string leaderId, string memberId, IEnumerable<Member> members)
        {
            ErrorCode = errorCode;
            Errors = ImmutableList<ErrorResponseCode>.Empty.Add(ErrorCode);
            GenerationId = generationId;
            GroupProtocol = groupProtocol;
            LeaderId = leaderId;
            MemberId = memberId;
            Members = ImmutableList<Member>.Empty.AddNotNullRange(members);
        }

        /// <inheritdoc />
        public IImmutableList<ErrorResponseCode> Errors { get; }

        public ErrorResponseCode ErrorCode { get; }

        /// <summary>
        /// The generation counter for completion of the join group phase.
        /// </summary>
        public int GenerationId { get; }

        /// <summary>
        /// The group protocol selected by the coordinator. Is this the name or the type??
        /// </summary>
        public string GroupProtocol { get; }

        /// <summary>
        /// The leader of the group.
        /// </summary>
        public string LeaderId { get; }

        /// <summary>
        /// The consumer id assigned by the group coordinator.
        /// </summary>
        public string MemberId { get; }

        public IImmutableList<Member> Members { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as JoinGroupResponse);
        }

        /// <inheritdoc />
        public bool Equals(JoinGroupResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return ErrorCode == other.ErrorCode 
                && GenerationId == other.GenerationId 
                && string.Equals(GroupProtocol, other.GroupProtocol) 
                && string.Equals(LeaderId, other.LeaderId) 
                && string.Equals(MemberId, other.MemberId) 
                && Members.HasEqualElementsInOrder(other.Members);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                var hashCode = (int) ErrorCode;
                hashCode = (hashCode*397) ^ GenerationId;
                hashCode = (hashCode*397) ^ (GroupProtocol?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (LeaderId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (MemberId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (Members?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(JoinGroupResponse left, JoinGroupResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(JoinGroupResponse left, JoinGroupResponse right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class Member : IEquatable<Member>
        {
            public Member(string memberId, IMemberMetadata metadata)
            {
                MemberId = memberId;
                Metadata = metadata;
            }

            public string MemberId { get; }
            public IMemberMetadata Metadata { get; }

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Member);
            }

            /// <inheritdoc />
            public bool Equals(Member other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(MemberId, other.MemberId) 
                    && Equals(Metadata, other.Metadata);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((MemberId?.GetHashCode() ?? 0)*397) ^ (Metadata?.GetHashCode() ?? 0);
                }
            }

            /// <inheritdoc />
            public static bool operator ==(Member left, Member right)
            {
                return Equals(left, right);
            }

            /// <inheritdoc />
            public static bool operator !=(Member left, Member right)
            {
                return !Equals(left, right);
            }
        }

    }
}