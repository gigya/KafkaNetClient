using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    internal class ManualTesting
    {
        private readonly KafkaOptions _options = new KafkaOptions(TestConfig.IntegrationUri, log: TestConfig.Log);

        /// <summary>
        /// These tests are for manual run. You need to stop the partition leader and then start it again and let it became the leader.        
        /// </summary>

        [Test]
        [Ignore("Disable auto topic create in our server")]
        public async Task NewlyCreatedTopicShouldRetryUntilBrokerIsAssigned()
        {
            var expectedTopic = Guid.NewGuid().ToString();
            var router = new Router(_options);
            var response = router.GetMetadataAsync(new MetadataRequest(expectedTopic), CancellationToken.None);
            var topic = (await response).Topics.FirstOrDefault();

            Assert.That(topic, Is.Not.Null);
            Assert.That(topic.TopicName, Is.EqualTo(expectedTopic));
            Assert.That(topic.ErrorCode, Is.EqualTo((int)ErrorResponseCode.None));
        }

        [Test]
        [Ignore("manual test")]
        public async Task ManualConsumerFailure()
        {
            var topicName = "TestTopicIssue13-3R-1P";
            using (var router = new Router(_options)) {
                var consumer = new Consumer(new Router(_options), new ConsumerConfiguration(maxPartitionFetchBytes: 10000));
                var offset = await router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None);

                var producer = new Producer(router);
                var send = SandMessageForever(producer, offset.TopicName, offset.PartitionId);
                var read = ReadMessageForever(consumer, offset.TopicName, offset.PartitionId, offset.Offset);
                await Task.WhenAll(send, read);
            }
        }

        private async Task SandMessageForever(IProducer producer, string topicName, int partitionId)
        {
            for (var id = 0;;) {
                try {
                    await producer.SendMessageAsync(new Message((++id).ToString()), topicName, partitionId, CancellationToken.None);
                    await Task.Delay(100);
                } catch (Exception ex) {
                    TestConfig.Log.Info(() => LogEvent.Create(ex, "can't send:"));
                }
            }
        }

        private  async Task ReadMessageForever(IConsumer consumer, string topicName, int partitionId, long offset)
        {
            while (true) {
                try {
                    var messages = await consumer.FetchMessagesAsync(topicName, partitionId, offset, 100, CancellationToken.None);

                    if (messages.Any()) {
                        foreach (var message in messages) {
                            TestConfig.Log.Info(() => LogEvent.Create($"Offset{message.Offset}"));
                        }
                        offset = messages.Max(x => x.Offset) + 3;
                    } else {
                        await Task.Delay(100);
                    }
                } catch (Exception ex) {
                    TestConfig.Log.Info(() => LogEvent.Create(ex, "can't read:"));
                }
            }
        }
    }
}