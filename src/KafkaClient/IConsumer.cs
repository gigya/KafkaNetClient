using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumer
    {
        /// <summary>
        /// Explicit fetch for topic/partition. This does not use consumer groups.
        /// </summary>
        Task<IImmutableList<Message>> FetchMessagesAsync(string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken);

        //Task CommitOffsetsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// The configuration for various limits and for consume defaults
        /// </summary>
        IConsumerConfiguration Configuration { get; }

        Task<IConsumerGroupMember> JoinConsumerGroupAsync(string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, CancellationToken cancellationToken, IConsumerGroupMember member = null);
        Task<IMemberAssignment> SyncGroupAsync(string groupId, string memberId, int generationId, string protocolType, IImmutableDictionary<string, IMemberMetadata> memberMetadata, CancellationToken cancellationToken);
        Task<ErrorResponseCode> SendHeartbeatAsync(string groupId, string memberId, int generationId, CancellationToken cancellationToken);
        Task LeaveConsumerGroupAsync(string groupId, string memberId, CancellationToken cancellationToken, bool awaitResponse = true);
    }
}