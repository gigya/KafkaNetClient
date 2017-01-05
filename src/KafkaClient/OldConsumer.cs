using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;

namespace KafkaClient
{
    /// <summary>
    /// Provides a basic consumer of one Topic across all partitions or over a given whitelist of partitions.
    ///
    /// TODO: provide automatic offset saving when the feature is available in 0.8.2 https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// TODO: replace _fetchResponseQueue to AsyncCollection!! it is not recommended to  use async and blocking code together look on https://github.com/StephenCleary/AsyncEx/blob/77b9711c2c5fd4ca28b220ce4c93d209eeca2b4a/Source/Nito.AsyncEx.Concurrent%20(NET4%2C%20Win8)/AsyncCollection.cs
    /// I don't use this consumer so i stop develop it i am using manual consumer instend
    /// </summary>
    [Obsolete("This is here for reference more than anything else -- it needs a rewrite")]
    public class OldConsumer : IKafkaClient
    {
        private readonly ConsumerOptions _options;
        private readonly BlockingCollection<Message> _fetchResponseQueue;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly TaskCompletionSource<int> _disposeTask;
        private readonly ConcurrentDictionary<int, Task> _partitionPollingIndex = new ConcurrentDictionary<int, Task>();
        private readonly ConcurrentDictionary<int, long> _partitionOffsetIndex = new ConcurrentDictionary<int, long>();

        private int _disposeCount;
        private int _ensureOneThread;
        private MetadataResponse.Topic _topic;

        public OldConsumer(ConsumerOptions options, params OffsetPosition[] positions)
        {
            _options = options;
            _fetchResponseQueue = new BlockingCollection<Message>(_options.ConsumerBufferSize);
            _disposeTask = new TaskCompletionSource<int>();
            SetOffsetPosition(positions);
        }

        public IRouter Router => _options.Router;

        /// <summary>
        /// Get the number of tasks created for consuming each partition.
        /// </summary>
        public int ConsumerTaskCount => _partitionPollingIndex.Count;

        /// <summary>
        /// Returns a blocking enumerable of messages received from Kafka.
        /// </summary>
        /// <returns>Blocking enumberable of messages from Kafka.</returns>
        public IEnumerable<Message> Consume(CancellationToken? cancellationToken = null)
        {
            _options.Log.Info(() => LogEvent.Create($"Consumer: Beginning consumption of topic/{_options.Topic}"));
            EnsurePartitionPollingThreads();
            return _fetchResponseQueue.GetConsumingEnumerable(cancellationToken ?? CancellationToken.None);
        }

        /// <summary>
        /// Force reset the offset position for a specific partition to a specific offset value.
        /// </summary>
        /// <param name="positions">Collection of positions to reset to.</param>
        public void SetOffsetPosition(params OffsetPosition[] positions)
        {
            foreach (var position in positions)
            {
                var temp = position;
                _partitionOffsetIndex.AddOrUpdate(position.PartitionId, i => temp.Offset, (i, l) => temp.Offset);
            }
        }

        /// <summary>
        /// Get the current running position (offset) for all consuming partition.
        /// </summary>
        /// <returns>List of positions for each consumed partitions.</returns>
        /// <remarks>Will only return data if the consumer is actively being consumed.</remarks>
        public List<OffsetPosition> GetOffsetPosition()
        {
            return _partitionOffsetIndex.Select(x => new OffsetPosition(x.Key, x.Value)).ToList();
        }

        private void EnsurePartitionPollingThreads()
        {
            try
            {
                if (Interlocked.Increment(ref _ensureOneThread) == 1)
                {
                    _options.Log.Debug(() => LogEvent.Create($"Consumer: Refreshing partitions for topic/{_options.Topic}"));
                    _topic = _options.Router.GetTopicMetadataAsync(_options.Topic, CancellationToken.None).Result;

                    //create one thread per partition, if they are in the white list.
                    foreach (var partition in _topic.Partitions)
                    {
                        var partitionId = partition.PartitionId;
                        if (_options.PartitionWhitelist.Count == 0 || _options.PartitionWhitelist.Any(x => x == partitionId))
                        {
                            _partitionPollingIndex.AddOrUpdate(partitionId,
                                                               i => ConsumeTopicPartitionAsync(_topic.TopicName, partitionId, CancellationToken.None),
                                                               (i, task) => task);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _options.Log.Error(LogEvent.Create(ex, $"Trying to setup consumer for topic/{_options.Topic}"));
            }
            finally
            {
                Interlocked.Decrement(ref _ensureOneThread);
            }
        }

        private Task ConsumeTopicPartitionAsync(string topic, int partitionId, CancellationToken cancellationToken)
        {
            bool needToRefreshMetadata = false;
            return Task.Run(async () =>
            {
                try
                {
                    var bufferSizeHighWatermark = FetchRequest.DefaultBufferSize;

                    _options.Log.Info(() => LogEvent.Create($"Consumer: Creating polling task for topic/{topic}/parition/{partitionId}"));
                    while (_disposeToken.IsCancellationRequested == false)
                    {
                        try
                        {
                            //after error
                            if (needToRefreshMetadata)
                            {
                                await _options.Router.GetTopicMetadataAsync(topic, cancellationToken).ConfigureAwait(false);
                                EnsurePartitionPollingThreads();
                                needToRefreshMetadata = false;
                            }
                            //get the current offset, or default to zero if not there.
                            long offset = 0;
                            _partitionOffsetIndex.AddOrUpdate(partitionId, i => offset, (i, currentOffset) =>
                            {
                                offset = currentOffset;
                                return currentOffset;
                            });

                            //build a fetch request for partition at offset
                            var fetch = new FetchRequest.Topic(topic, partitionId, offset, bufferSizeHighWatermark);
                            var fetchRequest = new FetchRequest(fetch, _options.MaxWaitTimeForMinimumBytes, _options.MinimumBytes);

                            //make request and post to queue
                            var route = _options.Router.GetTopicBroker(topic, partitionId);

                            var taskSend = route.Connection.SendAsync(fetchRequest, cancellationToken);

                            await Task.WhenAny(taskSend, _disposeTask.Task).ConfigureAwait(false);
                            if (_disposeTask.Task.IsCompleted) return;

                            //already done
                            var response = await taskSend;
                            if (response?.Topics.Count > 0)
                            {
                                // we only asked for one response
                                var fetchTopicResponse = response.Topics.FirstOrDefault();

                                if (fetchTopicResponse != null && fetchTopicResponse.Messages.Any()) {
                                    HandleResponseErrors(fetchRequest, fetchTopicResponse, route.Connection);

                                    foreach (var message in fetchTopicResponse.Messages)
                                    {
                                        await Task.Run(() => {
                                            // this is a block operation
                                            _fetchResponseQueue.Add(message, _disposeToken.Token);
                                        }, _disposeToken.Token).ConfigureAwait(false);

                                        if (_disposeToken.IsCancellationRequested) return;
                                    }

                                    var nextOffset = fetchTopicResponse.Messages.Last().Offset + 1;
                                    _partitionOffsetIndex.AddOrUpdate(partitionId, i => nextOffset, (i, l) => nextOffset);

                                    // sleep is not needed if responses were received
                                    continue;
                                }
                            }

                            //no message received from server wait a while before we try another long poll
                            await Task.Delay(_options.BackoffInterval, _disposeToken.Token);
                        }
                        catch (ConnectionException ex)
                        {
                            needToRefreshMetadata = true;
                            _options.Log.Error(LogEvent.Create(ex));
                        }
                        catch (BufferUnderRunException ex)
                        {
                            bufferSizeHighWatermark = (int)(ex.RequiredBufferSize * _options.FetchBufferMultiplier) + ex.MessageHeaderSize;
                            // ReSharper disable once AccessToModifiedClosure
                            _options.Log.Info(() => LogEvent.Create($"Buffer underrun: Increasing buffer size to {bufferSizeHighWatermark}"));
                        }
                        catch (FetchOutOfRangeException ex) when (ex.ErrorCode == ErrorResponseCode.OffsetOutOfRange)
                        {
                            //TODO this turned out really ugly.  Need to fix this section.
                            _options.Log.Error(LogEvent.Create(ex));
                            await FixOffsetOutOfRangeExceptionAsync(ex.Topic);
                        }
                        catch (CachedMetadataException ex)
                        {
                            // refresh our metadata and ensure we are polling the correct partitions
                            needToRefreshMetadata = true;
                            _options.Log.Error(LogEvent.Create(ex));
                        }

                        catch (TaskCanceledException)
                        {
                            //TODO :LOG
                        }
                        catch (Exception ex)
                        {
                            _options.Log.Error(LogEvent.Create(ex, $"Failed polling topic/{topic}/partition/{partitionId}: Polling will continue"));
                        }
                    }
                }
                finally
                {
                    _options.Log.Info(() => LogEvent.Create($"Consumer disabling polling task for topic/{topic}/partition/{partitionId}"));
                    Task tempTask;
                    _partitionPollingIndex.TryRemove(partitionId, out tempTask);
                }
            }, cancellationToken);
        }

        private void HandleResponseErrors(FetchRequest request, FetchResponse.Topic response, IConnection connection)
        {
            switch (response.ErrorCode)
            {
                case ErrorResponseCode.None:
                    return;

                case ErrorResponseCode.BrokerNotAvailable:
                case ErrorResponseCode.GroupCoordinatorNotAvailable:
                case ErrorResponseCode.LeaderNotAvailable:
                case ErrorResponseCode.NotLeaderForPartition:
                    throw new CachedMetadataException(
                        $"FetchResponse indicated we may have mismatched metadata ({response.ErrorCode})",
                        request.ExtractException(response.ErrorCode, connection?.Endpoint));

                default:
                    throw request.ExtractException(response.ErrorCode, connection?.Endpoint);
            }
        }

        private async Task FixOffsetOutOfRangeExceptionAsync(FetchRequest.Topic topic)
        {
            await _options.Router.GetTopicOffsetsAsync(topic.TopicName, 2, -1, CancellationToken.None)
                   .ContinueWith(t =>
                   {
                       try
                       {
                           var offsets = t.Result.Where(x => x.PartitionId == topic.PartitionId).ToList();
                           if (!offsets.Any()) return;

                           var minOffset = offsets.Min(o => o.Offset);
                           if (minOffset > topic.Offset)
                               SetOffsetPosition(new OffsetPosition(topic.PartitionId, minOffset));

                           var maxOffset = offsets.Max(o => o.Offset);
                           if (maxOffset < topic.Offset)
                               SetOffsetPosition(new OffsetPosition(topic.PartitionId, maxOffset));
                       }
                       catch (Exception ex)
                       {
                           _options.Log.Error(LogEvent.Create(ex, $"Failed to fix the offset out of range exception on topic/{topic.TopicName}/partition/{topic.PartitionId}: Polling will continue"));
                       }
                   });
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _options.Log?.Debug(() => LogEvent.Create("Consumer: Disposing..."));
            _disposeToken.Cancel();
            _disposeTask.SetResult(1);
            //wait for all threads to unwind
            foreach (var task in _partitionPollingIndex.Values.Where(task => task != null)) {
                task.Wait(TimeSpan.FromSeconds(5));
            }

            using (_options.Router) {
                using (_disposeToken)
                { }
            }
        }
    }
}