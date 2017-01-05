namespace KafkaClient.Protocol
{
    /// <summary>
    /// Heartbeat Request (Version: 0) => group_id group_generation_id member_id 
    ///   group_id => STRING           -- The group id.
    ///   group_generation_id => INT32 -- The generation of the group.
    ///   member_id => STRING          -- The member id assigned by the group coordinator.
    /// 
    /// see http://kafka.apache.org/protocol.html#protocol_messages
    /// 
    /// Once a member has joined and synced, it will begin sending periodic heartbeats to keep itself in the group. If a heartbeat has *not* been 
    /// received by the coordinator with the configured session timeout, the member will be kicked out of the group.
    /// </summary>
    public class HeartbeatRequest : GroupRequest, IRequest<HeartbeatResponse>
    {
        /// <inheritdoc />
        public HeartbeatRequest(string groupId, int groupGenerationId, string memberId) 
            : base(ApiKeyRequestType.Heartbeat, groupId, memberId, groupGenerationId)
        {
        }
    }
}