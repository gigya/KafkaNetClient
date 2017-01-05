using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// SyncGroupRequest => GroupId GroupGenerationId MemberId [GroupAssignment]
    ///   GroupId => string
    ///   GroupGenerationId => int32
    ///   MemberId => string
    ///   GroupAssignment => MemberId MemberAssignment
    ///     MemberId => string
    ///     MemberAssignment => bytes
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// </summary>
    public class SyncGroupRequest : GroupRequest, IRequest<SyncGroupResponse>, IEquatable<SyncGroupRequest>
    {
        /// <inheritdoc />
        public SyncGroupRequest(string groupId, int groupGenerationId, string memberId, IEnumerable<GroupAssignment> groupAssignments) 
            : base(ApiKeyRequestType.SyncGroup, groupId, memberId, groupGenerationId)
        {
            GroupAssignments = ImmutableList<GroupAssignment>.Empty.AddNotNullRange(groupAssignments);
        }

        public IImmutableList<GroupAssignment> GroupAssignments { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as SyncGroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(SyncGroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && GroupAssignments.HasEqualElementsInOrder(other.GroupAssignments);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                return (base.GetHashCode()*397) ^ (GroupAssignments?.GetHashCode() ?? 0);
            }
        }

        /// <inheritdoc />
        public static bool operator ==(SyncGroupRequest left, SyncGroupRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(SyncGroupRequest left, SyncGroupRequest right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class GroupAssignment : IEquatable<GroupAssignment>
        {
            public GroupAssignment(string memberId, IMemberAssignment memberAssignment)
            {
                MemberId = memberId;
                MemberAssignment = memberAssignment;
            }

            public string MemberId { get; }
            public IMemberAssignment MemberAssignment { get; }

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as GroupAssignment);
            }

            /// <inheritdoc />
            public bool Equals(GroupAssignment other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(MemberId, other.MemberId) 
                    && Equals(MemberAssignment, other.MemberAssignment);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((MemberId?.GetHashCode() ?? 0)*397) ^ (MemberAssignment?.GetHashCode() ?? 0);
                }
            }

            /// <inheritdoc />
            public static bool operator ==(GroupAssignment left, GroupAssignment right)
            {
                return Equals(left, right);
            }

            /// <inheritdoc />
            public static bool operator !=(GroupAssignment left, GroupAssignment right)
            {
                return !Equals(left, right);
            }
        }

    }
}