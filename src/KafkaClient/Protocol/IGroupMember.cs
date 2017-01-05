namespace KafkaClient.Protocol
{
    public interface IGroupMember
    {
        /// <summary>
        /// The group id.
        /// </summary>
        string GroupId { get; }

        /// <summary>
        /// The member id assigned by the group coordinator (ie one of the kafka servers).
        /// In the join group phase, this is empty (ie "") for first timers, but rejoining members should use the same memberId (from previous generations).
        /// </summary>
        string MemberId { get; }
    }
}