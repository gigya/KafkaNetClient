using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    public class GzipTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(TestConfig.IntegrationUri, log: TestConfig.Log);

        private Connection GetKafkaConnection()
        {
            var endpoint = new ConnectionFactory().Resolve(_options.ServerUris.First(), _options.Log);
            var configuration = _options.ConnectionConfiguration;
            return new Connection(new TcpSocket(endpoint, configuration), configuration, _options.Log);
        }

        [Test]
        public async Task EnsureGzipCompressedMessageCanSend()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    TestConfig.Log.Info(() => LogEvent.Create(">> Start EnsureGzipCompressedMessageCanSend"));
                    using (var conn1 = GetKafkaConnection()) {
                        await conn1.SendAsync(new MetadataRequest(topicName), CancellationToken.None);
                    }

                    TestConfig.Log.Info(() => LogEvent.Create(">> Start GetTopicMetadataAsync"));
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    TestConfig.Log.Info(() => LogEvent.Create(">> End GetTopicMetadataAsync"));
                    var conn = router.GetTopicBroker(topicName, 0);

                    var request = new ProduceRequest(new ProduceRequest.Payload(topicName, 0, new [] {
                                        new Message("0", "1"),
                                        new Message("1", "1"),
                                        new Message("2", "1")
                                    }, MessageCodec.CodecGzip));
                    TestConfig.Log.Info(() => LogEvent.Create(">> start SendAsync"));
                    var response = await conn.Connection.SendAsync(request, CancellationToken.None);
                    TestConfig.Log.Info(() => LogEvent.Create("end SendAsync"));
                    Assert.That(response.Errors.Any(e => e != ErrorResponseCode.None), Is.False);
                    TestConfig.Log.Info(() => LogEvent.Create("start dispose"));
                    TestConfig.Log.Info(() => LogEvent.Create(">> End EnsureGzipCompressedMessageCanSend"));
                });
            }
        }

        [Test]
        public async Task EnsureGzipCanDecompressMessageFromKafka()
        {
            const int numberOfMessages = 3;
            const int partitionId = 0;

            TestConfig.Log.Info(() => LogEvent.Create(">> Start EnsureGzipCanDecompressMessageFromKafka"));
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    OffsetResponse.Topic offset;
                    var messages = new List<Message>();
                    using (var producer = new Producer(router, new ProducerConfiguration(batchSize: numberOfMessages))) {
                        offset = await producer.Router.GetTopicOffsetAsync(topicName, 0, CancellationToken.None) ?? new OffsetResponse.Topic(topicName, partitionId, offset: 0);
                        for (var i = 0; i < numberOfMessages; i++) {
                            messages.Add(new Message(i.ToString()));
                        }
                        TestConfig.Log.Info(() => LogEvent.Create(">> Start Produce"));
                        await producer.SendMessagesAsync(messages, topicName, partitionId, new SendMessageConfiguration(codec: MessageCodec.CodecGzip), CancellationToken.None);
                        TestConfig.Log.Info(() => LogEvent.Create(">> End Produce"));
                    }
                    TestConfig.Log.Info(() => LogEvent.Create(">> Start Consume"));
                    using (var consumer = new Consumer(router)) {
                        var results = await consumer.FetchMessagesAsync(offset, messages.Count, CancellationToken.None);
                        TestConfig.Log.Info(() => LogEvent.Create(">> End Consume"));
                        Assert.That(results, Is.Not.Null);
                        Assert.That(results.Count, Is.EqualTo(messages.Count));
                        for (var i = 0; i < messages.Count; i++) {
                            Assert.That(results[i].Value.ToUtf8String(), Is.EqualTo(i.ToString()));
                        }
                    }
                    TestConfig.Log.Info(() => LogEvent.Create(">> End EnsureGzipCanDecompressMessageFromKafka"));
                });
            }
        }
    }
}