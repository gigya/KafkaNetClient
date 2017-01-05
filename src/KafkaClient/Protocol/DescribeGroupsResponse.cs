using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DescribeGroupsResponse => [Group]
    ///  Group => ErrorCode GroupId State ProtocolType Protocol [Member]
    ///   ErrorCode => int16
    ///   GroupId => string
    ///   State => string
    ///   ProtocolType => string
    ///   Protocol => string
    ///   Member => MemberId ClientId ClientHost MemberMetadata MemberAssignment
    ///     MemberId => string
    ///     ClientId => string
    ///     ClientHost => string
    ///     MemberMetadata => bytes
    ///     MemberAssignment => bytes
    ///
    /// From http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class DescribeGroupsResponse : IResponse, IEquatable<DescribeGroupsResponse>
    {
        public DescribeGroupsResponse(IEnumerable<Group> groups)
        {
            Groups = ImmutableList<Group>.Empty.AddNotNullRange(groups);
            Errors = ImmutableList<ErrorResponseCode>.Empty.AddRange(Groups.Select(g => g.ErrorCode));
        }

        public IImmutableList<ErrorResponseCode> Errors { get; }

        public IImmutableList<Group> Groups { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as DescribeGroupsResponse);
        }

        /// <inheritdoc />
        public bool Equals(DescribeGroupsResponse other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Groups.HasEqualElementsInOrder(other.Groups);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Groups?.GetHashCode() ?? 0;
        }

        /// <inheritdoc />
        public static bool operator ==(DescribeGroupsResponse left, DescribeGroupsResponse right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(DescribeGroupsResponse left, DescribeGroupsResponse right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class Group : IEquatable<Group>
        {
            public Group(ErrorResponseCode errorCode, string groupId, string state, string protocolType, string protocol, IEnumerable<Member> members)
            {
                ErrorCode = errorCode;
                GroupId = groupId;
                State = state;
                ProtocolType = protocolType;
                Protocol = protocol;
                Members = ImmutableList<Member>.Empty.AddNotNullRange(members);
            }

            public ErrorResponseCode ErrorCode { get; }
            public string GroupId { get; }

            public static class States
            {
                public const string Dead = "Dead";
                public const string Stable = "Stable";
                public const string AwaitingSync = "AwaitingSync";
                public const string PreparingRebalance = "PreparingRebalance";
                public const string NoActiveGroup = "";
            }

            /// <summary>
            /// The current state of the group (one of: Dead, Stable, AwaitingSync, or PreparingRebalance, or empty if there is no active group)
            /// </summary>
            public string State { get; }

            /// <summary>
            /// The current group protocol type (will be empty if there is no active group)
            /// </summary>
            public string ProtocolType { get; }

            /// <summary>
            /// The current group protocol (only provided if the group is Stable)
            /// </summary>
            public string Protocol { get; }

            /// <summary>
            /// Current group members (only provided if the group is not Dead)
            /// </summary>
            public IImmutableList<Member> Members { get; }

            #region Equality

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as Group);
            }

            /// <inheritdoc />
            public bool Equals(Group other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return ErrorCode == other.ErrorCode 
                       && String.Equals(GroupId, other.GroupId) 
                       && String.Equals(State, other.State) 
                       && String.Equals(ProtocolType, other.ProtocolType) 
                       && String.Equals(Protocol, other.Protocol) 
                       && Members.HasEqualElementsInOrder(other.Members);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = (int) ErrorCode;
                    hashCode = (hashCode*397) ^ (GroupId?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (State?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (ProtocolType?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Protocol?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (Members?.GetHashCode() ?? 0);
                    return hashCode;
                }
            }

            /// <inheritdoc />
            public static bool operator ==(Group left, Group right)
            {
                return Equals(left, right);
            }

            /// <inheritdoc />
            public static bool operator !=(Group left, Group right)
            {
                return !Equals(left, right);
            }

            #endregion
        }

        public class Member : IEquatable<Member>
        {
            public Member(string memberId, string clientId, string clientHost, IMemberMetadata memberMetadata, IMemberAssignment memberAssignment)
            {
                MemberId = memberId;
                ClientId = clientId;
                ClientHost = clientHost;
                MemberMetadata = memberMetadata;
                MemberAssignment = memberAssignment;
            }

            /// <summary>
            /// The memberId assigned by the coordinator
            /// </summary>
            public string MemberId { get; }

            /// <summary>
            /// The client id used in the member's latest join group request
            /// </summary>
            public string ClientId { get; }

            /// <summary>
            /// The client host used in the request session corresponding to the member's join group.
            /// </summary>
            public string ClientHost { get; }

            /// <summary>
            /// The metadata corresponding to the current group protocol in use (will only be present if the group is stable).
            /// </summary>
            public IMemberMetadata MemberMetadata { get; }

            /// <summary>
            /// The current assignment provided by the group leader (will only be present if the group is stable).
            /// </summary>
            public IMemberAssignment MemberAssignment { get; }

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
                return String.Equals(MemberId, (string) other.MemberId) 
                    && String.Equals(ClientId, (string) other.ClientId) 
                    && String.Equals(ClientHost, (string) other.ClientHost) 
                    && Equals(MemberMetadata, other.MemberMetadata) 
                    && Equals(MemberAssignment, other.MemberAssignment);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    var hashCode = MemberId?.GetHashCode() ?? 0;
                    hashCode = (hashCode*397) ^ (ClientId?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (ClientHost?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (MemberMetadata?.GetHashCode() ?? 0);
                    hashCode = (hashCode*397) ^ (MemberAssignment?.GetHashCode() ?? 0);
                    return hashCode;
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