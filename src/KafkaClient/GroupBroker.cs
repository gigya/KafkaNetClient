using KafkaClient.Connections;

namespace KafkaClient
{
    public class GroupBroker : Broker
    {
        public GroupBroker(string groupId, int brokerId, IConnection connection)
            : base(brokerId, connection)
        {
            GroupId = groupId;
        }

        public string GroupId { get; }

        public override string ToString() => $"{base.ToString()} group/{GroupId}";

        public override bool Equals(object obj)
        {
            return Equals(obj as GroupBroker);
        }

        public bool Equals(GroupBroker other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && string.Equals(GroupId, other.GroupId);
        }

        public override int GetHashCode()
        {
            unchecked {
                return (BrokerId * 397) ^ (GroupId?.GetHashCode() ?? 0);
            }
        }

        public static bool operator ==(GroupBroker left, GroupBroker right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(GroupBroker left, GroupBroker right)
        {
            return !Equals(left, right);
        }
    }
}