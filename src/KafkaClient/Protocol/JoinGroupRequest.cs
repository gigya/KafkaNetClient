using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Assignment;
using KafkaClient.Common;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// JoinGroup Request => group_id session_timeout *rebalance_timeout member_id protocol_type [GroupProtocol] 
    ///  *rebalance_timeout only applies to version 1 (Kafka 0.10.1) and above
    ///   group_id => STRING           -- The group id.
    ///   session_timeout => INT32     -- The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms.
    ///   rebalance_timeout => INT32   -- The maximum time that the coordinator will wait for each member to rejoin when rebalancing the group
    ///   member_id => STRING          -- The assigned consumer id or an empty string for a new consumer.
    ///   protocol_type => STRING      -- Unique name for class of protocols implemented by group (ie "consumer")
    ///   GroupProtocol => ProtocolName ProtocolMetadata
    ///     protocol_name => STRING    -- ie AssignmentStrategy for "consumer" type. protocol_name != protocol_type. It's a subtype of sorts.
    ///     protocol_metadata => BYTES -- <see cref="ConsumerProtocolMetadata"/>
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages for details
    /// 
    /// The join group request is used by a client to become a member of a group. 
    /// When new members join an existing group, all previous members are required to rejoin by sending a new join group request. 
    /// When a member first joins the group, the memberId will be empty (i.e. ""), but a rejoining member should use the same memberId 
    /// from the previous generation. 
    /// 
    /// The SessionTimeout field is used to indicate client liveness. If the coordinator does not receive at least one heartbeat (see below) 
    /// before expiration of the session timeout, then the member will be removed from the group. Prior to version 0.10.1, the session timeout 
    /// was also used as the timeout to complete a needed rebalance. Once the coordinator begins rebalancing, each member in the group has up 
    /// to the session timeout in order to send a new JoinGroup request. If they fail to do so, they will be removed from the group. In 0.10.1, 
    /// a new version of the JoinGroup request was created with a separate RebalanceTimeout field. Once a rebalance begins, each client has up 
    /// to this duration to rejoin, but note that if the session timeout is lower than the rebalance timeout, the client must still continue 
    /// to send heartbeats.
    /// 
    /// The ProtocolType field defines the embedded protocol that the group implements. The group coordinator ensures that all members in 
    /// the group support the same protocol type. The meaning of the protocol name and metadata contained in the GroupProtocols field depends 
    /// on the protocol type. Note that the join group request allows for multiple protocol/metadata pairs. This enables rolling upgrades 
    /// without downtime. The coordinator chooses a single protocol which all members support. The upgraded member includes both the new 
    /// version and the old version of the protocol. Once all members have upgraded, the coordinator will choose whichever protocol is listed 
    /// first in the GroupProtocols array.
    /// </summary>
    public class JoinGroupRequest : Request, IRequest<JoinGroupResponse>, IGroupMember, IEquatable<JoinGroupRequest>
    {
        public JoinGroupRequest(string groupId, TimeSpan sessionTimeout, string memberId, string protocolType, IEnumerable<GroupProtocol> groupProtocols, TimeSpan? rebalanceTimeout = null) 
            : base(ApiKeyRequestType.JoinGroup, protocolType: protocolType)
        {
            GroupId = groupId;
            SessionTimeout = sessionTimeout;
            RebalanceTimeout = rebalanceTimeout ?? SessionTimeout;
            MemberId = memberId;
            GroupProtocols = ImmutableList<GroupProtocol>.Empty.AddNotNullRange(groupProtocols);
        }

        public TimeSpan SessionTimeout { get; }
        public TimeSpan RebalanceTimeout { get; }

        public IImmutableList<GroupProtocol> GroupProtocols { get; }
        /// <inheritdoc />
        public string GroupId { get; }

        /// <inheritdoc />
        public string MemberId { get; }

        #region Equality

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as JoinGroupRequest);
        }

        /// <inheritdoc />
        public bool Equals(JoinGroupRequest other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) 
                && SessionTimeout.Equals(other.SessionTimeout) 
                && RebalanceTimeout.Equals(other.RebalanceTimeout) 
                && string.Equals(GroupId, other.GroupId) 
                && string.Equals(MemberId, other.MemberId)
                && GroupProtocols.HasEqualElementsInOrder(other.GroupProtocols);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode*397) ^ SessionTimeout.GetHashCode();
                hashCode = (hashCode*397) ^ RebalanceTimeout.GetHashCode();
                hashCode = (hashCode*397) ^ (GroupProtocols?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (GroupId?.GetHashCode() ?? 0);
                hashCode = (hashCode*397) ^ (MemberId?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <inheritdoc />
        public static bool operator ==(JoinGroupRequest left, JoinGroupRequest right)
        {
            return Equals(left, right);
        }

        /// <inheritdoc />
        public static bool operator !=(JoinGroupRequest left, JoinGroupRequest right)
        {
            return !Equals(left, right);
        }

        #endregion

        public class GroupProtocol : IEquatable<GroupProtocol>
        {
            public GroupProtocol(IMemberMetadata metadata)
            {
                Metadata = metadata;
            }

            public string Name => Metadata.AssignmentStrategy;
            public IMemberMetadata Metadata { get; }

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as GroupProtocol);
            }

            /// <inheritdoc />
            public bool Equals(GroupProtocol other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(Name, other.Name) 
                    && Equals(Metadata, other.Metadata);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                unchecked {
                    return ((Name?.GetHashCode() ?? 0)*397) ^ (Metadata?.GetHashCode() ?? 0);
                }
            }

            /// <inheritdoc />
            public static bool operator ==(GroupProtocol left, GroupProtocol right)
            {
                return Equals(left, right);
            }

            /// <inheritdoc />
            public static bool operator !=(GroupProtocol left, GroupProtocol right)
            {
                return !Equals(left, right);
            }
        }

    }
}