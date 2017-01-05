using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NSubstitute;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    public class ProducerTests
    {
        [Test]
        public async Task ProducerShouldNotExpectResponseWhenAckIsZero()
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var sendTask = producer.SendMessageAsync(
                            new Message(Guid.NewGuid().ToString()), TestConfig.TopicName(), null,
                            new SendMessageConfiguration(acks: 0), CancellationToken.None);

                        await Task.WhenAny(sendTask, Task.Delay(TimeSpan.FromMinutes(2)));

                        Assert.That(sendTask.Status, Is.EqualTo(TaskStatus.RanToCompletion));
                    }
                });
            }
        }

        [Test]
        public async Task SendAsyncShouldGetOneResultForMessage()
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var result = await producer.SendMessagesAsync(new[] { new Message(Guid.NewGuid().ToString()) }, TestConfig.TopicName(), CancellationToken.None);

                        Assert.That(result.Count, Is.EqualTo(1));
                    }
                });
            }
        }

        [Test]
        public async Task SendAsyncShouldGetAResultForEachPartitionSentTo()
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var messages = new[] { new Message("1"), new Message("2"), new Message("3") };
                        var result = await producer.SendMessagesAsync(messages, TestConfig.TopicName(), CancellationToken.None);

                        Assert.That(result.Count, Is.EqualTo(messages.Distinct().Count()));

                        Assert.That(result.Count, Is.EqualTo(messages.Count()));
                    }
                });
            }
        }

        [Test]
        public async Task SendAsyncShouldGetOneResultForEachPartitionThroughBatching()
        {
            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri))) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var tasks = new[] {
                            producer.SendMessageAsync(new Message("1"), TestConfig.TopicName(), CancellationToken.None),
                            producer.SendMessageAsync(new Message("2"), TestConfig.TopicName(), CancellationToken.None),
                            producer.SendMessageAsync(new Message("3"), TestConfig.TopicName(), CancellationToken.None),
                        };

                        await Task.WhenAll(tasks);

                        var result = tasks.Select(x => x.Result).Distinct().ToList();
                        Assert.That(result.Count, Is.EqualTo(tasks.Length));
                    }
                });
            }
        }

        [Test]
        public async Task ProducerAckLevel()
        {
            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel0 = await producer.SendMessageAsync(new Message("Ack Level 0"), topicName, 0, new SendMessageConfiguration(acks: 0), CancellationToken.None);
                        Assert.AreEqual(responseAckLevel0.Offset, -1);
                        var responseAckLevel1 = await producer.SendMessageAsync(new Message("Ack Level 1"), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        Assert.That(responseAckLevel1.Offset, Is.GreaterThan(-1));
                    }
                });
            }
        }

        [Test]
        public async Task ProducerAckLevel1ResponseOffsetShouldBeEqualToLastOffset()
        {
            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log )) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 = await producer.SendMessageAsync(new Message("Ack Level 1"), topicName, 0, new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        var offsetResponse = await producer.Router.GetTopicOffsetsAsync(topicName, CancellationToken.None);
                        var maxOffset = offsetResponse.First(x => x.PartitionId == 0);
                        Assert.AreEqual(responseAckLevel1.Offset, maxOffset.Offset - 1);
                    }
                });
            }
        }

        [Test]
        public async Task ProducerLastResposeOffsetAckLevel1ShouldBeEqualsToLastOffset()
        {
            using (var router = new Router(TestConfig.IntegrationUri, log: TestConfig.Log)) {
                await router.TemporaryTopicAsync(async topicName => {
                    using (var producer = new Producer(router)) {
                        var responseAckLevel1 =
                            await producer.SendMessagesAsync(
                                new[] { new Message("Ack Level 1"), new Message("Ack Level 1") }, topicName, 0,
                                new SendMessageConfiguration(acks: 1), CancellationToken.None);
                        var offsetResponse = await producer.Router.GetTopicOffsetsAsync(
                            topicName, CancellationToken.None);
                        var maxOffset = offsetResponse.First(x => x.PartitionId == 0);

                        Assert.AreEqual(responseAckLevel1.Last().Offset, maxOffset.Offset - 1);
                    }
                });
            }
        }

        [Test]
        public async Task ProducerShouldUsePartitionIdInsteadOfMessageKeyToChoosePartition()
        {
            var partitionSelector = Substitute.For<IPartitionSelector>();
            partitionSelector.Select(null, null)
                             .ReturnsForAnyArgs(_ => _.Arg<MetadataResponse.Topic>().Partitions.Single(p => p.PartitionId == 1));

            using (var router = new Router(new KafkaOptions(TestConfig.IntegrationUri, partitionSelector: partitionSelector))) {
                await router.TemporaryTopicAsync(async topicName => {
                var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);
                    using (var producer = new Producer(router)) {
                        //message should send to PartitionId and not use the key to Select Broker Route !!
                        for (var i = 0; i < 20; i++) {
                            await producer.SendMessageAsync(new Message(i.ToString(), "key"), offset.TopicName, offset.PartitionId, CancellationToken.None);
                        }
                    }

                    using (var consumer = new Consumer(router)) {
                        using (var source = new CancellationTokenSource()) {
                            var i = 0;
                            await consumer.FetchAsync(
                                offset, 20, message =>
                                {
                                    Assert.That(message.Value.ToUtf8String(), Is.EqualTo(i++.ToString()));
                                    if (i >= 20) {
                                        source.Cancel();
                                    }
                                    return Task.FromResult(0);
                                }, source.Token);
                        }
                    }
                });
            }
        }
    }
}