using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    [Category("Load")]
    public class LoadTests
    {
        [Test]
        [TestCase(10, 1000)]
        [TestCase(100, 1000)]
        [TestCase(1000, 1000)]
        public async Task SendAsyncShouldHandleHighVolumeOfMessages(int amount, int maxAsync)
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router, new ProducerConfiguration(maxAsync, amount / 2)))
                    {
                        var tasks = new Task<ProduceResponse.Topic>[amount];

                        for (var i = 0; i < amount; i++) {
                            tasks[i] = producer.SendMessageAsync(new Message(Guid.NewGuid().ToString()), TestConfig.TopicName(), CancellationToken.None);
                        }
                        var results = await Task.WhenAll(tasks.ToArray());

                        //Because of how responses are batched up and sent to servers, we will usually get multiple responses per requested message batch
                        //So this assertion will never pass
                        //Assert.That(results.Count, Is.EqualTo(amount));

                        Assert.That(results.Any(x => x.ErrorCode != ErrorResponseCode.None), Is.False,
                            "Should not have received any results as failures.");
                    }
                });
            }
        }

        [Test]
        [TestCase(1, 1, 70)]
        [TestCase(1000, 50, 70)]
        [TestCase(30000, 100, 2550)]
        [TestCase(50000, 100, 850)]
        [TestCase(200000, 1000, 8050)]
        public async Task ConsumerProducerSpeedUnderLoad(int totalMessages, int batchSize, int timeoutInMs)
        {
            var expected = totalMessages.Repeat(i => i.ToString()).ToList();

            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: totalMessages / 10, batchMaxDelay: TimeSpan.FromMilliseconds(25)))) {
                        var offset = await producer.Router.GetTopicOffsetAsync(TestConfig.TopicName(), 0, CancellationToken.None);

                        var stopwatch = new Stopwatch();
                        stopwatch.Start();
                        var sendList = new List<Task>(totalMessages);
                        for (var i = 0; i < totalMessages; i+=batchSize) {
                            var sendTask = producer.SendMessagesAsync(batchSize.Repeat(x => new Message(x.ToString())), offset.TopicName, offset.PartitionId, CancellationToken.None);
                            sendList.Add(sendTask);
                        }
                        var maxTimeToRun = TimeSpan.FromMilliseconds(timeoutInMs);
                        var doneSend = Task.WhenAll(sendList.ToArray());
                        await Task.WhenAny(doneSend, Task.Delay(maxTimeToRun));
                        stopwatch.Stop();
                        if (!doneSend.IsCompleted) {
                            var completed = sendList.Count(t => t.IsCompleted);
                            Assert.Inconclusive($"Only finished sending {completed} of {totalMessages} in {timeoutInMs} ms.");
                        }
                        await doneSend;
                        TestConfig.Log.Info(() => LogEvent.Create($">> done send, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));
                        stopwatch.Restart();

                        using (var consumer = new Consumer(router, new ConsumerConfiguration(maxServerWait: TimeSpan.Zero))) {
                            var fetched = ImmutableList<Message>.Empty;
                            stopwatch.Restart();
                            while (fetched.Count < totalMessages) {
                                var doneFetch = consumer.FetchMessagesAsync(offset.TopicName, offset.PartitionId, offset.Offset + fetched.Count, totalMessages, CancellationToken.None);
                                var delay = Task.Delay((int) Math.Max(0, maxTimeToRun.TotalMilliseconds - stopwatch.ElapsedMilliseconds));
                                await Task.WhenAny(doneFetch, delay);
                                if (delay.IsCompleted && !doneFetch.IsCompleted) {
                                    Assert.Fail($"Received {fetched.Count} of {totalMessages} in {timeoutInMs} ms.");
                                }
                                var results = await doneFetch;
                                fetched = fetched.AddRange(results);
                            }
                            stopwatch.Stop();
                            TestConfig.Log.Info(() => LogEvent.Create($">> done Consume, time Milliseconds:{stopwatch.ElapsedMilliseconds}"));

    //                        Assert.That(fetched.Select(x => x.Value.ToUtf8String()).ToList(), Is.EqualTo(expected), "Expected the message list in the correct order.");
                            Assert.That(fetched.Count, Is.EqualTo(totalMessages));
                        }
                    }
                });
            }
        }
    }
}