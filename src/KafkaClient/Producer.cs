﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// Provides a simplified high level API for producing messages on a topic.
    /// </summary>
    public class Producer : IProducer
    {
        private readonly CancellationTokenSource _stopToken = new CancellationTokenSource();
        private readonly AsyncCollection<ProduceTopicTask> _asyncCollection;
        private readonly SemaphoreSlim _semaphoreMaximumAsync;
        private readonly Task _batchSendTask;

        private int _inFlightCount;

        /// <summary>
        /// Get the number of messages sitting in the buffer waiting to be sent.
        /// </summary>
        public int BufferedCount => _asyncCollection.Count;

        /// <summary>
        /// Get the number of messages staged for Async upload.
        /// </summary>
        public int InFlightCount => _inFlightCount;

        /// <summary>
        /// Get the number of active async threads sending messages.
        /// </summary>
        public int ActiveSenders => Configuration.RequestParallelization - _semaphoreMaximumAsync.CurrentCount;

        /// <inheritdoc />
        public IBrokerRouter BrokerRouter { get; }

        public IProducerConfiguration Configuration { get; }

        /// <summary>
        /// Construct a Producer class.
        /// </summary>
        /// <param name="brokerRouter">The router used to direct produced messages to the correct partition.</param>
        /// <param name="configuration">The configuration parameters.</param>
        /// <remarks>
        /// The <see cref="IProducerConfiguration.RequestParallelization"/> parameter provides a mechanism for minimizing the amount of 
        /// async requests in flight at any one time by blocking the caller requesting the async call. This effectively puts an upper 
        /// limit on the amount of times a caller can call SendMessagesAsync before the caller is blocked.
        ///
        /// The <see cref="IProducerConfiguration.BatchSize"/> parameter provides a way to limit the max amount of memory the driver uses 
        /// should the send pipeline get overwhelmed and the buffer starts to fill up.  This is an inaccurate limiting memory use as the 
        /// amount of memory actually used is dependant on the general message size being buffered.
        ///
        /// A message will start its timeout countdown as soon as it is added to the producer async queue. If there are a large number of
        /// messages sitting in the async queue then a message may spend its entire timeout cycle waiting in this queue and never getting
        /// attempted to send to Kafka before a timeout exception is thrown.
        /// </remarks>
        public Producer(IBrokerRouter brokerRouter, IProducerConfiguration configuration = null)
        {
            BrokerRouter = brokerRouter;
            Configuration = configuration ?? new ProducerConfiguration();
            _asyncCollection = new AsyncCollection<ProduceTopicTask>();
            _semaphoreMaximumAsync = new SemaphoreSlim(Configuration.RequestParallelization, Configuration.RequestParallelization);
            _batchSendTask = Task.Run(BatchSendAsync, _stopToken.Token);
        }

        /// <inheritdoc />
        public async Task<ProduceTopic[]> SendMessagesAsync(IEnumerable<Message> messages, string topicName, int? partition, ISendMessageConfiguration configuration, CancellationToken cancellationToken)
        {
            var produceTopicTasks = messages.Select(message => new ProduceTopicTask(topicName, partition, message, configuration ?? Configuration.SendDefaults, cancellationToken)).ToArray();
            _asyncCollection.AddRange(produceTopicTasks); 
            // TODO: cancellation should also remove from the collection if they aren't yet in progress
            // Finally, they should skip the semaphore wait in this case (or at least short circuit it)
            return await Task.WhenAll(produceTopicTasks.Select(x => x.Tcs.Task)).ConfigureAwait(false);
        }

        /// <summary>
        /// Stops the producer from accepting new messages, waiting for in-flight messages to be sent before returning.
        /// </summary>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            // block incoming data
            _asyncCollection.CompleteAdding();
            await Task.WhenAny(_batchSendTask, Task.Delay(Configuration.StopTimeout, cancellationToken)).ConfigureAwait(false);
            _stopToken.Cancel();
        }

        private async Task BatchSendAsync()
        {
            BrokerRouter.Log.InfoFormat("Producer sending task starting");
            try {
                while (IsNotDisposedOrHasMessagesToProcess()) {
                    List<ProduceTopicTask> batch = null;

                    try
                    {
                        try {
                            await _asyncCollection.OnHasDataAvailable(_stopToken.Token).ConfigureAwait(false);
                            batch = await _asyncCollection.TakeAsync(Configuration.BatchSize, Configuration.BatchMaxDelay, _stopToken.Token).ConfigureAwait(false);
                        } catch (OperationCanceledException) {
                            //TODO log that the operation was canceled, this only happens during a dispose
                        }

                        if (_asyncCollection.IsCompleted && _asyncCollection.Count > 0) {
                            batch = batch ?? new List<ProduceTopicTask>(_asyncCollection.Count);

                            //Drain any messages remaining in the queue and add them to the send batch
                            batch.AddRange(_asyncCollection.Take());
                        }
                        if (batch != null) {
                            await ProduceAndSendBatchAsync(batch, _stopToken.Token).ConfigureAwait(false);
                        }
                    } catch (Exception ex) {
                        batch?.ForEach(x => x.Tcs.TrySetException(ex));
                    }
                }
            } finally {
                BrokerRouter.Log.InfoFormat("Producer sending task ending");
            }

        }

        private bool IsNotDisposedOrHasMessagesToProcess()
        {
            return _asyncCollection.IsCompleted == false || _asyncCollection.Count > 0;
        }

        private async Task ProduceAndSendBatchAsync(List<ProduceTopicTask> messages, CancellationToken cancellationToken)
        {
            Interlocked.Add(ref _inFlightCount, messages.Count);

            var topics = messages.GroupBy(batch => batch.TopicName).Select(batch => batch.Key).ToArray();
            await BrokerRouter.GetTopicMetadataAsync(topics, cancellationToken).ConfigureAwait(false);

            //we must send a different produce request for each ack level and timeout combination.
            foreach (var ackLevelBatch in messages.GroupBy(batch => new { batch.Acks, Timeout = batch.AckTimeout }))
            {
                var messageByRouter = ackLevelBatch.Select(batch => new
                {
                    TopicMessage = batch,
                    ackLevelBatch.Key.Acks,
                    Route = batch.Partition.HasValue ? BrokerRouter.GetBrokerRoute(batch.TopicName, batch.Partition.Value) : BrokerRouter.GetBrokerRoute(batch.TopicName, batch.Message.Key)
                }).GroupBy(x => new { x.Route, Topic = x.TopicMessage.TopicName, x.TopicMessage.Codec, x.Acks });

                var batches = new List<ProduceTopicTaskBatch>();
                foreach (var group in messageByRouter)
                {
                    var payload = new Payload(group.Key.Topic, group.Key.Route.PartitionId, group.Select(x => x.TopicMessage.Message), group.Key.Codec);
                    var request = new ProduceRequest(payload, ackLevelBatch.Key.Timeout, ackLevelBatch.Key.Acks);

                    await _semaphoreMaximumAsync.WaitAsync(cancellationToken).ConfigureAwait(false);

                    var sendGroupTask = BrokerRouter.SendAsync(request, group.Key.Topic, group.Key.Route.PartitionId, cancellationToken);
                    var batch = new ProduceTopicTaskBatch(group.Key.Route, group.Key.Acks, sendGroupTask, group.Select(_ => _.TopicMessage));

                    // ReSharper disable UnusedVariable
                    //ensure the async is released as soon as each task is completed //TODO: remove it from ack level 0 , don't like it
                    var continuation = batch.ReceiveTask.ContinueWith(t => { _semaphoreMaximumAsync.Release(); }, cancellationToken);
                    // ReSharper restore UnusedVariable

                    batches.Add(batch);
                }

                try
                {
                    await Task.WhenAll(batches.Select(x => x.ReceiveTask)).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    BrokerRouter.Log.ErrorFormat("Exception[{0}] stacktrace[{1}]", ex.Message, ex.StackTrace);
                }

                await SetResult(batches).ConfigureAwait(false);
                Interlocked.Add(ref _inFlightCount, messages.Count * -1);
            }
        }

        private async Task SetResult(List<ProduceTopicTaskBatch> sendTasks)
        {
            foreach (var sendTask in sendTasks)
            {
                try
                {
                    // already done don't need to await but it none blocking syntax
                    var batchResult = await sendTask.ReceiveTask.ConfigureAwait(false);
                    var numberOfMessage = sendTask.MessagesSent.Count;
                    for (int i = 0; i < numberOfMessage; i++) {
                        if (sendTask.Acks == 0) {
                            var response = new ProduceTopic(sendTask.Route.TopicName, sendTask.Route.PartitionId, ErrorResponseCode.NoError, -1);
                            sendTask.MessagesSent[i].Tcs.SetResult(response);
                        } else {
                            // HACK: assume there is at most one ...
                            var topic = batchResult.Topics.SingleOrDefault();
                            if (topic == null) {
                                sendTask.MessagesSent[i].Tcs.SetResult(null);
                            } else {
                                var response = new ProduceTopic(topic.TopicName, topic.PartitionId, topic.ErrorCode, topic.Offset + i, topic.Timestamp);
                                sendTask.MessagesSent[i].Tcs.SetResult(response);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    BrokerRouter.Log.ErrorFormat("failed to send batch Topic[{0}] ackLevel[{1}] partition[{2}] EndPoint[{3}] Exception[{4}] stacktrace[{5}]", sendTask.Route.TopicName, sendTask.Acks, sendTask.Route.PartitionId, sendTask.Route.Connection.Endpoint, ex.Message, ex.StackTrace);
                    sendTask.MessagesSent.ForEach(x => x.Tcs.TrySetException(ex));
                }
            }
        }

        public void Dispose()
        {
            // block incoming data
            _asyncCollection.CompleteAdding();
            _stopToken.Cancel();

            // cleanup
            using (_stopToken) {
                using (BrokerRouter)
                {
                }
            }
        }

        private class ProduceTopicTask : CancellableTask<ProduceTopic>
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

        private class ProduceTopicTaskBatch
        {
            public ProduceTopicTaskBatch(BrokerRoute route, short acks, Task<ProduceResponse> receiveTask, IEnumerable<ProduceTopicTask> messagesSent)
            {
                Route = route;
                Acks = acks;
                ReceiveTask = receiveTask;
                MessagesSent = ImmutableList<ProduceTopicTask>.Empty.AddNotNullRange(messagesSent);
            }

            public short Acks { get; }
            public BrokerRoute Route { get; }
            public Task<ProduceResponse> ReceiveTask { get; }
            public ImmutableList<ProduceTopicTask> MessagesSent { get; }
        }
    }
}