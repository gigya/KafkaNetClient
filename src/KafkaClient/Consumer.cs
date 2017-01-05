using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// Simple consumer with access to a single topic
    /// </summary>
    public class Consumer : IConsumer, IDisposable
    {
        private readonly IRouter _router;
        private readonly bool _leaveRouterOpen;
        private readonly CancellationTokenSource _stopToken;

        public Consumer(KafkaOptions options)
            : this(new Router(options), options.ConsumerConfiguration, options.ConnectionConfiguration.Encoders, false)
        {
        }

        public Consumer(IRouter router, IConsumerConfiguration configuration = null, IImmutableDictionary<string, IMembershipEncoder> encoders = null, bool leaveRouterOpen = true)
        {
            _stopToken = new CancellationTokenSource();
            _router = router;
            _leaveRouterOpen = leaveRouterOpen;
            Configuration = configuration ?? new ConsumerConfiguration();
            _localMessages = ImmutableList<Message>.Empty;
            _encoders = encoders ?? ImmutableDictionary<string, IMembershipEncoder>.Empty;
        }

        private readonly IImmutableDictionary<string, IMembershipEncoder> _encoders;

        public IConsumerConfiguration Configuration { get; }

        private ImmutableList<Message> _localMessages;

        /// <inheritdoc />
        public async Task<IImmutableList<Message>> FetchMessagesAsync(string topicName, int partitionId, long offset, int maxCount, CancellationToken cancellationToken)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "must be >= 0");

            // Previously fetched messages may contain everything we need
            var localIndex = _localMessages.FindIndex(m => m.Offset == offset);
            if (0 <= localIndex && localIndex + maxCount <= _localMessages.Count) return _localMessages.GetRange(localIndex, maxCount);

            var localCount = (0 <= localIndex && localIndex < _localMessages.Count) ? _localMessages.Count - localIndex : 0;
            var request = new FetchRequest(new FetchRequest.Topic(topicName, partitionId, offset + localCount, Configuration.MaxPartitionFetchBytes), 
                Configuration.MaxFetchServerWait, Configuration.MinFetchBytes, Configuration.MaxFetchBytes);
            var response = await _router.SendAsync(request, topicName, partitionId, cancellationToken).ConfigureAwait(false);
            var topic = response.Topics.SingleOrDefault();

            if (topic?.Messages?.Count == 0) return ImmutableList<Message>.Empty;

            if (localCount > 0) {
                // Previously fetched messages contain some of what we need, so append
                _localMessages = (ImmutableList<Message>)_localMessages.AddNotNullRange(topic?.Messages);
            } else {
                localIndex = 0;
                _localMessages = topic?.Messages?.ToImmutableList() ?? ImmutableList<Message>.Empty;
            }
            return _localMessages.GetRange(localIndex, Math.Min(maxCount, _localMessages.Count));
        }

        public void Dispose()
        {
            using (_stopToken) {
                if (_leaveRouterOpen) return;
                using (_router)
                {
                }
            }
        }

        public async Task<IConsumerGroupMember> JoinConsumerGroupAsync(string groupId, string protocolType, IEnumerable<IMemberMetadata> metadata, CancellationToken cancellationToken, IConsumerGroupMember member = null)
        {
            if (!_encoders.ContainsKey(protocolType ?? "")) throw new ArgumentOutOfRangeException(nameof(metadata), $"ProtocolType {protocolType} is unknown");

            var protocols = metadata.Select(m => new JoinGroupRequest.GroupProtocol(m));
            var request = new JoinGroupRequest(groupId, Configuration.GroupHeartbeat, member?.MemberId ?? "", protocolType, protocols, Configuration.GroupRebalanceTimeout);
            var response = await _router.SendAsync(request, groupId, cancellationToken);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }

            if (member != null) {
                member.OnRejoin(response);
                return member;
            }
            return new ConsumerGroupMember(this, request, response, _router.Log);
        }

        public async Task LeaveConsumerGroupAsync(string groupId, string memberId, CancellationToken cancellationToken, bool awaitResponse = true)
        {
            var request = new LeaveGroupRequest(groupId, memberId, awaitResponse);
            var response = await _router.SendAsync(request, groupId, cancellationToken);
            if (awaitResponse && !response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
        }

        public async Task<ErrorResponseCode> SendHeartbeatAsync(string groupId, string memberId, int generationId, CancellationToken cancellationToken)
        {
            var request = new HeartbeatRequest(groupId, generationId, memberId);
            var response = await _router.SendAsync(request, groupId, cancellationToken);
            return response.ErrorCode;
        }

        public async Task<IMemberAssignment> SyncGroupAsync(string groupId, string memberId, int generationId, string protocolType, IImmutableDictionary<string, IMemberMetadata> memberMetadata, CancellationToken cancellationToken)
        {
            IEnumerable<SyncGroupRequest.GroupAssignment> groupAssignments = null;
            if (memberMetadata?.Count > 0) {
                var metadata = memberMetadata.First().Value;
                var encoder = _encoders[protocolType];
                var assigner = encoder.GetAssignor(metadata.AssignmentStrategy);
                var memberAssignment = await assigner.AssignMembersAsync(_router, memberMetadata, cancellationToken);
                groupAssignments = memberAssignment.Select(assignment => new SyncGroupRequest.GroupAssignment(assignment.Key, assignment.Value));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, groupAssignments);
            var response = await _router.SendAsync(request, groupId, cancellationToken);
            if (!response.ErrorCode.IsSuccess()) {
                throw request.ExtractExceptions(response);
            }
            return response.MemberAssignment;
        }
    }
}