using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;

namespace KafkaClient
{
    public interface IConsumerGroupMember : IGroupMember, IDisposable
    {
        int GenerationId { get; }
        bool IsLeader { get; }
        string ProtocolType { get; }

        void OnRejoin(JoinGroupResponse response);

        //Task<IImmutableList<Message>> FetchMessagesAsync(int maxCount, CancellationToken cancellationToken);
        //Task CommitOffsetsAsync(CancellationToken cancellationToken);
    }
}