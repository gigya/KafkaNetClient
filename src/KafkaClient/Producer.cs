using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using Nito.AsyncEx;

namespace KafkaClient
{
    /// <summary>
    /// Provides a simplified high level API for producing messages on a topic.
    /// </summary>
    public class Producer : IProducer
    {
        private readonly bool _leaveRouterOpen;
        private int _stopCount = 0;
        private readonly CancellationTokenSource _stopToken;
        private readonly AsyncProducerConsumerQueue<ProduceTopicTask> _produceMessageQueue;
        private readonly SemaphoreSlim _produceRequestSemaphore;
        private readonly Task _batchSendTask;

        private int _sendingMessageCount;
        private ImmutableList<ProduceTopicTask> _batch = ImmutableList<ProduceTopicTask>.Empty;

        /// <summary>
        /// Get the number of messages sitting in the buffer waiting to be sent.
        /// </summary>
        public int BufferedMessageCount => _sendingMessageCount - _batch.Count;

        /// <summary>
        /// Get the number of messages staged for Async Request.
        /// </summary>
        public int InFlightMessageCount => _batch.Count;

        /// <summary>
        /// Get the number of active async threads sending messages.
        /// </summary>
        public int ActiveSenders => Configuration.RequestParallelization - _produceRequestSemaphore.CurrentCount;

        /// <inheritdoc />
        public IRouter Router { get; }

        public IProducerConfiguration Configuration { get; }

        public Producer(KafkaOptions options)
            : this(new Router(options), options.ProducerConfiguration, false)
        {
        }

        ///  <summary>
        ///  Construct a Producer class.
        ///  </summary>
        ///  <param name="router">The router used to direct produced messages to the correct partition.</param>
        ///  <param name="configuration">The configuration parameters.</param>
        /// <param name="leaveRouterOpen">Whether to dispose the router when the producer is disposed.</param>
        /// <remarks>
        ///  The <see cref="IProducerConfiguration.RequestParallelization"/> parameter provides a mechanism for minimizing the amount of 
        ///  async requests in flight at any one time by blocking the caller requesting the async call. This effectively puts an upper 
        ///  limit on the amount of times a caller can call SendMessagesAsync before the caller is blocked.
        /// 
        ///  The <see cref="IProducerConfiguration.BatchSize"/> parameter provides a way to limit the max amount of memory the driver uses 
        ///  should the send pipeline get overwhelmed and the buffer starts to fill up.  This is an inaccurate limiting memory use as the 
        ///  amount of memory actually used is dependant on the general message size being buffered.
        /// 
        ///  A message will start its timeout countdown as soon as it is added to the producer async queue. If there are a large number of
        ///  messages sitting in the async queue then a message may spend its entire timeout cycle waiting in this queue and never getting
        ///  attempted to send to Kafka before a timeout exception is thrown.
        ///  </remarks>
        public Producer(IRouter router, IProducerConfiguration configuration = null, bool leaveRouterOpen = true)
        {
            _leaveRouterOpen = leaveRouterOpen;
            Router = router;
            Configuration = configuration ?? new ProducerConfiguration();
            _produceMessageQueue = new AsyncProducerConsumerQueue<ProduceTopicTask>();
            _produceRequestSemaphore = new SemaphoreSlim(Configuration.RequestParallelization, Configuration.RequestParallelization);
            _stopToken = new CancellationTokenSource();
            _batchSendTask = Task.Run(BatchSendAsync, _stopToken.Token);
        }

        /// <inheritdoc />
        public async Task<ImmutableList<ProduceResponse.Topic>> SendMessagesAsync(IEnumerable<Message> messages, string topicName, int? partition, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            var produceTopicTasks = messages.Select(message => new ProduceTopicTask(topicName, partition, message, configuration ?? Configuration.SendDefaults, cancellationToken)).ToArray();
            Interlocked.Add(ref _sendingMessageCount, produceTopicTasks.Length);
            try {
                await _produceMessageQueue.EnqueueRangeAsync(produceTopicTasks, cancellationToken);
                var topics = await Task.WhenAll(produceTopicTasks.Select(x => x.Tcs.Task)).ConfigureAwait(false);
                return topics.ToImmutableList();
            } catch (InvalidOperationException ex) {
                throw new ObjectDisposedException("Cannot send messages after Stopped or Disposed", ex);
            } finally {
                Interlocked.Add(ref _sendingMessageCount, -produceTopicTasks.Length);
            }
        }

        private async Task BatchSendAsync()
        {
            Router.Log.Info(() => LogEvent.Create("Producer sending task starting"));
            try {
                while (!_stopToken.IsCancellationRequested) {
                    _batch = ImmutableList<ProduceTopicTask>.Empty;
                    try {
                        _batch = await GetNextBatchAsync();
                        if (_batch.IsEmpty) {
                            if (_stopCount > 0) {
                                Router.Log.Info(() => LogEvent.Create("Producer stopping and nothing available to send"));
                                break;
                            }
                        } else {
                            foreach (var codec in new[] {MessageCodec.CodecNone, MessageCodec.CodecGzip}) {
                                var filteredBatch = _batch.Where(_ => _.Codec == codec).ToImmutableList();
                                if (filteredBatch.IsEmpty) continue;

                                Router.Log.Debug(() => LogEvent.Create($"Producer compiled batch({filteredBatch.Count}) on codec {codec}{(_stopCount == 0 ? "" : ": producer is stopping")}"));
                                var batchToken = filteredBatch[0].CancellationToken;
                                if (filteredBatch.TrueForAll(p => p.CancellationToken == batchToken)) {
                                    using (var merged = new MergedCancellation(_stopToken.Token, batchToken)) {
                                        await SendBatchWithCodecAsync(filteredBatch, codec, merged.Token).ConfigureAwait(false);
                                    }
                                } else {
                                    await SendBatchWithCodecAsync(filteredBatch, codec, _stopToken.Token).ConfigureAwait(false);
                                }
                                _batch = _batch.Where(_ => _.Codec != codec).ToImmutableList(); // so the below catch doesn't update tasks that are already completed
                            }
                        }
                    } catch (Exception ex) {
                        Router.Log.Info(() => LogEvent.Create(ex));
                        _batch.ForEach(x => x.Tcs.TrySetException(ex));
                    }
                }
            } catch(Exception ex) { 
                Router.Log.Warn(() => LogEvent.Create(ex, "Error during producer send"));
            } finally {
                Router.Log.Info(() => LogEvent.Create("Producer sending task ending"));
            }
        }

        private async Task<ImmutableList<ProduceTopicTask>> GetNextBatchAsync()
        {
            var batch = ImmutableList<ProduceTopicTask>.Empty;
            try {
                await _produceMessageQueue.OutputAvailableAsync(_stopToken.Token).ConfigureAwait(false);
                using (var cancellation = new TimedCancellation(_stopToken.Token, Configuration.BatchMaxDelay)) {
                    while (batch.Count < Configuration.BatchSize && !cancellation.Token.IsCancellationRequested) {
                        // Try rather than simply Take (in case the collection has been closed and is not empty)
                        var result = await _produceMessageQueue.DequeueAsync(cancellation.Token);

                        if (result.CancellationToken.IsCancellationRequested) {
                            result.Tcs.SetCanceled();
                        } else {
                            batch = batch.Add(result);
                        }
                    }
                }
            } catch (InvalidOperationException) { // From DequeueAsync
            } catch (OperationCanceledException) { // cancellation token fired while attempting to get tasks: normal behavior
            }
            return batch;
        }

        private async Task SendBatchWithCodecAsync(IImmutableList<ProduceTopicTask> produceTasks, MessageCodec codec, CancellationToken cancellationToken)
        {
            await Router.GetTopicMetadataAsync(produceTasks.Select(m => m.TopicName), cancellationToken).ConfigureAwait(false);

            // we must send a different produce request for each ack level and timeout combination.
            // we must also send requests to the correct broker / endpoint
            var endpointGroups = produceTasks.Select(
                ptt => new {
                    ProduceTask = ptt,
                    Route = ptt.Partition.HasValue
                        ? Router.GetTopicBroker(ptt.TopicName, ptt.Partition.Value)
                        : Router.GetTopicBroker(ptt.TopicName, ptt.Message.Key)
                })
                .GroupBy(_ => new {_.ProduceTask.Acks, _.ProduceTask.AckTimeout, _.Route.Connection.Endpoint});

            var sendBatches = new List<ProduceTaskBatch>();
            foreach (var endpointGroup in endpointGroups) {
                var produceTasksByTopicPayload = endpointGroup
                    .GroupBy(_ => new TopicPartition(_.Route.TopicName, _.Route.PartitionId))
                    .ToImmutableDictionary(g => g.Key, g => g.Select(_ => _.ProduceTask).ToImmutableList());
                var messageCount = produceTasksByTopicPayload.Values.Sum(_ => _.Count);
                var payloads = produceTasksByTopicPayload.Select(p => new ProduceRequest.Payload(p.Key.TopicName, p.Key.PartitionId, p.Value.Select(_ => _.Message), codec));
                var request = new ProduceRequest(payloads, endpointGroup.Key.AckTimeout, endpointGroup.Key.Acks);
                Router.Log.Debug(() => LogEvent.Create($"Produce request for topics{request.Payloads.Aggregate("", (buffer, p) => $"{buffer} {p}")} with {messageCount} messages"));

                var connection = endpointGroup.Select(_ => _.Route).First().Connection; // they'll all be the same since they're grouped by this
                await _produceRequestSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

                // TODO: what about retryability like for the broker router?? Need this to be robust to node failures
                var sendGroupTask = connection.SendAsync(request, cancellationToken);
                // ReSharper disable once UnusedVariable
                var continuation = sendGroupTask.ContinueWith(t => _produceRequestSemaphore.Release(), CancellationToken.None);
                sendBatches.Add(new ProduceTaskBatch(connection.Endpoint, endpointGroup.Key.Acks, sendGroupTask, produceTasksByTopicPayload));
            }

            try {
                await Task.WhenAll(sendBatches.Select(batch => batch.ReceiveTask)).ConfigureAwait(false);
            } catch (Exception ex) {
                Router.Log.Error(LogEvent.Create(ex));
            }
            await SetResult(sendBatches).ConfigureAwait(false);
        }

        private async Task SetResult(IEnumerable<ProduceTaskBatch> batches)
        {
            foreach (var batch in batches) {
                try {
                    var batchResult = await batch.ReceiveTask.ConfigureAwait(false); // await triggers exceptions correctly
                    if (batch.Acks == 0 && batchResult == null) {
                        foreach (var topic in batch.TasksByTopicPayload.Keys) {
                            foreach (var task in batch.TasksByTopicPayload[topic]) {
                                task.Tcs.SetResult(new ProduceResponse.Topic(topic.TopicName, topic.PartitionId, ErrorResponseCode.None, -1));
                            }
                        }
                        return;
                    }
                    var resultTopics = batchResult.Topics.ToImmutableDictionary(t => new TopicPartition(t.TopicName, t.PartitionId));
                    Router.Log.Debug(() => LogEvent.Create($"Produce response for topics{resultTopics.Keys.Aggregate("", (buffer, p) => $"{buffer} {p}")}"));

                    foreach (var topic in batch.TasksByTopicPayload.Keys.Except(resultTopics.Keys)) {
                        Router.Log.Warn(() => LogEvent.Create($"No response included for produce batch topic/{topic.TopicName}/partition/{topic.PartitionId} on {batch.Endpoint}"));
                        foreach (var task in batch.TasksByTopicPayload[topic]) {
                            task.Tcs.SetResult(null);
                        }
                    }

                    foreach (var pair in resultTopics) {
                        var topic = pair.Value;
                        ImmutableList<ProduceTopicTask> tasks;
                        if (!batch.TasksByTopicPayload.TryGetValue(pair.Key, out tasks)) {
                            Router.Log.Error(LogEvent.Create($"Extra response to produce batch topic/{topic.TopicName}/partition/{topic.PartitionId} on {batch.Endpoint}"));
                            continue;
                        }

                        var offsetCount = 0;
                        foreach (var task in tasks) {
                            task.Tcs.SetResult(batch.Acks == 0
                                ? new ProduceResponse.Topic(topic.TopicName, topic.PartitionId, ErrorResponseCode.None, -1)
                                : new ProduceResponse.Topic(topic.TopicName, topic.PartitionId, topic.ErrorCode, topic.Offset + offsetCount, topic.Timestamp));
                            offsetCount += 1;
                        }
                    }
                } catch (Exception ex) {
                    Router.Log.Error(LogEvent.Create(ex, $"failed to send batch to {batch.Endpoint} with acks {batch.Acks}"));
                    foreach (var productTopicTask in batch.TasksByTopicPayload.Values.SelectMany(_ => _)) {
                        productTopicTask.Tcs.TrySetException(ex);
                    }
                }
            }
        }

        /// <summary>
        /// Stops the producer from accepting new messages, waiting for in-flight messages to be sent before returning.
        /// </summary>
        public async Task<bool> StopAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _stopCount) != 1) return false;

            Router.Log.Info(() => LogEvent.Create("Producer stopping"));
            _produceMessageQueue.CompleteAdding(); // block incoming data
            await Task.WhenAny(_batchSendTask, Task.Delay(Configuration.StopTimeout, cancellationToken)).ConfigureAwait(false);
            _stopToken.Cancel();
            return true;
        }

        public void Dispose()
        {
            if (!AsyncContext.Run(() => StopAsync(new CancellationToken(true)))) return;

            // cleanup
            using (_stopToken) {
                if (!_leaveRouterOpen) {
                    using (Router)
                    {
                    }
                }
            }
        }

        private class ProduceTopicTask : CancellableTask<ProduceResponse.Topic>
        {
            public ProduceTopicTask(string topicName, int? partition, Message message, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
                : base(cancellationToken)
            {
                TopicName = topicName;
                Partition = partition;
                Message = message;
                Codec = configuration.Codec;
                Acks = configuration.Acks;
                AckTimeout = configuration.AckTimeout;
            }

            // where
            public string TopicName { get; }
            public int? Partition { get; }

            // what
            public Message Message { get; }
            public MessageCodec Codec { get; }

            // confirmation
            public short Acks { get; }
            public TimeSpan AckTimeout { get; }
        }

        private class ProduceTaskBatch
        {
            public ProduceTaskBatch(Endpoint endpoint, short acks, Task<ProduceResponse> receiveTask, ImmutableDictionary<TopicPartition, ImmutableList<ProduceTopicTask>> tasksByTopicPayload)
            {
                Endpoint = endpoint;
                Acks = acks;
                ReceiveTask = receiveTask;
                TasksByTopicPayload = tasksByTopicPayload;
            }

            public Endpoint Endpoint { get; }
            public short Acks { get; }
            public Task<ProduceResponse> ReceiveTask { get; }
            public ImmutableDictionary<TopicPartition, ImmutableList<ProduceTopicTask>> TasksByTopicPayload { get; }
        }
    }
}