using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using Nito.AsyncEx;

namespace KafkaClient
{
    public class ConsumerGroupMember : IConsumerGroupMember
    {
        private readonly IConsumer _consumer;

        public ConsumerGroupMember(IConsumer consumer, JoinGroupRequest request, JoinGroupResponse response, ILog log = null)
        {
            _consumer = consumer;
            _log = log ?? TraceLog.Log;

            GroupId = request.GroupId;
            MemberId = response.MemberId;
            ProtocolType = request.ProtocolType;

            OnRejoin(response);

            // This thread will heartbeat on the appropriate frequency
            _heartbeatDelay = TimeSpan.FromMilliseconds(request.SessionTimeout.TotalMilliseconds / 2);
            _heartbeatTimeout = request.SessionTimeout;
            _heartbeatTask = Task.Factory.StartNew(DedicatedHeartbeatTask, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private int _disposeCount;
        private int _activeHeartbeatCount = 0;
        private readonly Task _heartbeatTask;
        private readonly TimeSpan _heartbeatDelay;
        private readonly TimeSpan _heartbeatTimeout;
        private readonly ILog _log;
        private ImmutableDictionary<string, IMemberMetadata> _memberMetadata = ImmutableDictionary<string, IMemberMetadata>.Empty;
        private IMemberAssignment _memberAssignment;

        public string GroupId { get; }
        public string MemberId { get; }

        public bool IsLeader { get; private set; }
        public int GenerationId { get; private set; }

        /// <summary>
        /// State machine for Coordinator
        /// 
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
        /// </summary>
        /// <remarks>
        ///               +--------------------+
        ///               |                    |
        ///           +--->        Down        |
        ///           |   |                    |
        ///           |   +---------+----------+
        ///  Timeout  |             |
        ///  expires  |             | JoinGroup/Heartbeat
        ///  with     |             | received
        ///  no       |             v
        ///  group    |   +---------+----------+
        ///  activity |   |                    +---v JoinGroup/Heartbeat
        ///           |   |     Initialize     |   | return
        ///           |   |                    +---v coordinator not ready
        ///           |   +---------+----------+
        ///           |             |
        ///           |             | After reading
        ///           |             | group state
        ///           |             v
        ///           |   +---------+----------+
        ///           +---+                    +---v Heartbeat/SyncGroup
        ///               |       Stable       |   | from
        ///           +--->                    +---v active generation
        ///           |   +---------+----------+
        ///           |             |
        ///           |             | JoinGroup
        ///           |             | received
        ///           |             v
        ///           |   +---------+----------+
        ///           |   |                    |
        ///           |   |       Joining      |
        ///           |   |                    |
        /// Leader    |   +---------+----------+
        /// SyncGroup |             |
        /// or        |             | JoinGroup received
        /// session   |             | from all members
        /// timeout   |             v
        ///           |   +---------+----------+
        ///           |   |                    |
        ///           +---+      AwaitSync     |
        ///               |                    |
        ///               +--------------------+
        /// </remarks>
        private enum CoordinatorState
        {
            Down,
            Initialize,
            Stable,
            Joining,
            AwaitSync
        }

        /// <summary>
        /// See https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal for details
        /// </summary>
        private async Task DedicatedHeartbeatTask()
        {
            try {
                // only allow one heartbeat to execute, dump out all other requests
                if (Interlocked.Increment(ref _activeHeartbeatCount) != 1) return;

                CoordinatorState? state = CoordinatorState.Stable;
                var response = ErrorResponseCode.None;
                var lastHeartbeat = DateTimeOffset.UtcNow;
                while (!(_disposeToken.IsCancellationRequested || IsOverdue(lastHeartbeat) || IsUnrecoverable(response))) {
                    try {
                        if (state == null) { // only update state if it was previously reset -- this allows for one state to move explicitly to another
                            state = GetCoordinatorState(response);
                        }
                        switch (state) {
                            case CoordinatorState.Down:
                            case CoordinatorState.Initialize:
                            case CoordinatorState.Stable:
                                await Task.Delay(_heartbeatDelay, _disposeToken.Token).ConfigureAwait(false);
                                break;

                            case CoordinatorState.Joining:
                                await RejoinGroupAsync(_disposeToken.Token).ConfigureAwait(false);
                                state = CoordinatorState.AwaitSync;
                                lastHeartbeat = DateTimeOffset.UtcNow;
                                continue;

                            case CoordinatorState.AwaitSync:
                                await SyncGroupAsync(_disposeToken.Token).ConfigureAwait(false);
                                state = CoordinatorState.Stable;
                                lastHeartbeat = DateTimeOffset.UtcNow;
                                continue;

                            default:
                                await Task.Delay(1, _disposeToken.Token).ConfigureAwait(false);
                                break;
                        }

                        state = null; // clear so it gets set from the heartbeat response
                        response = await _consumer.SendHeartbeatAsync(GroupId, MemberId, GenerationId, _disposeToken.Token).ConfigureAwait(false);
                        if (response.IsSuccess()) {
                            lastHeartbeat = DateTimeOffset.UtcNow;
                        }
                    } catch (Exception ex) {
                        if (!(ex is TaskCanceledException)) {
                            _log.Warn(() => LogEvent.Create(ex));
                        }
                    }
                }
                await LeaveGroupAsync(CancellationToken.None);
            } catch (Exception ex) {
                if (!(ex is TaskCanceledException)) {
                    _log.Warn(() => LogEvent.Create(ex));
                }
            } finally {
                Interlocked.Decrement(ref _activeHeartbeatCount);
                _log.Info(() => LogEvent.Create($"Stopped heartbeat for {GroupId}/{MemberId}"));
            }
        }

        private bool IsOverdue(DateTimeOffset lastHeartbeat)
        {
            return _heartbeatTimeout < DateTimeOffset.UtcNow - lastHeartbeat;
        }

        private bool IsUnrecoverable(ErrorResponseCode? response)
        {
            return response == ErrorResponseCode.IllegalGeneration 
                || response == ErrorResponseCode.UnknownMemberId;
        }

        private CoordinatorState? GetCoordinatorState(ErrorResponseCode? response)
        {
            switch (response) {
                case ErrorResponseCode.GroupLoadInProgress:          return CoordinatorState.Down;
                case ErrorResponseCode.GroupCoordinatorNotAvailable: return CoordinatorState.Initialize;
                case ErrorResponseCode.None:                         return CoordinatorState.Stable;
                case ErrorResponseCode.RebalanceInProgress:          return CoordinatorState.Joining;
                    // could be AwaitSync state -- nothing to distinguish them
                case null:                                           return CoordinatorState.AwaitSync;

                default:                                             return null;
                    // no idea ...
            }
        }

        private async Task RejoinGroupAsync(CancellationToken cancellationToken)
        {
            // on success, this will call OnRejoin before returning
            await _consumer.JoinConsumerGroupAsync(GroupId, ProtocolType, _memberMetadata.Values, cancellationToken, this);
        }

        public void OnRejoin(JoinGroupResponse response)
        {
            if (response.MemberId != MemberId) throw new ArgumentOutOfRangeException(nameof(response), $"Member is not valid ({MemberId} != {response.MemberId})");

            // TODO: async lock
            IsLeader = response.LeaderId == MemberId;
            GenerationId = response.GenerationId;
            _memberMetadata = IsLeader 
                ? ImmutableDictionary<string, IMemberMetadata>.Empty.AddRange(response.Members.Select(m => new KeyValuePair<string, IMemberMetadata>(m.MemberId, m.Metadata))) 
                : ImmutableDictionary<string, IMemberMetadata>.Empty;
        }

        private async Task SyncGroupAsync(CancellationToken cancellationToken)
        {
            _memberAssignment = await _consumer.SyncGroupAsync(GroupId, MemberId, GenerationId, ProtocolType, IsLeader ? _memberMetadata : ImmutableDictionary<string, IMemberMetadata>.Empty, cancellationToken);
        }

        /// <summary>
        /// Leave the consumer group and stop heartbeats.
        /// </summary>
        public async Task LeaveGroupAsync(CancellationToken cancellationToken)
        {
            //skip multiple calls to dispose
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _disposeToken.Cancel();
            if (cancellationToken == CancellationToken.None) {
                await Task.WhenAny(_heartbeatTask, Task.Delay(TimeSpan.FromSeconds(1), cancellationToken));
                await _consumer.LeaveConsumerGroupAsync(GroupId, MemberId, cancellationToken, false);
            } else {
                await _heartbeatTask.WaitAsync(cancellationToken);
                await _consumer.LeaveConsumerGroupAsync(GroupId, MemberId, cancellationToken);
            }
        }

        public void Dispose()
        {
            AsyncContext.Run(() => LeaveGroupAsync(CancellationToken.None));
        }

        public string ProtocolType { get; }
    }
}