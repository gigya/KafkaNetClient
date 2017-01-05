using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient.Assignment
{
    public class ConsumerAssignor : MembershipAssignor<ConsumerProtocolMetadata, ConsumerMemberAssignment>
    {
        public static ImmutableList<IMembershipAssignor> Assignors { get; } = ImmutableList<IMembershipAssignor>.Empty.Add(new ConsumerAssignor());

        public const string Strategy = "simple";

        public ConsumerAssignor() : base(Strategy)
        {
        }

        protected override async Task<IImmutableDictionary<string, ConsumerMemberAssignment>> AssignAsync(IRouter router, IImmutableDictionary<string, ConsumerProtocolMetadata> memberMetadata, CancellationToken cancellationToken)
        {
            var topicNames = memberMetadata.Values.SelectMany(m => m.Subscriptions).Distinct().ToList();
            var topicMetadata = await router.GetTopicMetadataAsync(topicNames, cancellationToken);

            var keys = memberMetadata.Keys.ToImmutableArray();
            var assignments = memberMetadata.Keys.ToDictionary(_ => _, _ => new List<TopicPartition>());
            var index = -1;
            foreach (var partition in topicMetadata.SelectMany(t => t.Partitions.Select(p => new TopicPartition(t.TopicName, p.PartitionId))).OrderBy(_ => _.PartitionId).ToList()) {
                if (++index >= keys.Length) break;

                var key = keys[index];
                assignments[key].Add(partition);
            }

            return assignments.ToImmutableDictionary(pair => pair.Key, pair => new ConsumerMemberAssignment(pair.Value));
        }
    }
}