using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Integration
{
    [TestFixture]
    public class ManagementTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(TestConfig.IntegrationUri, log: TestConfig.Log);

        [Test]
        public async Task OffsetFetchRequestOfNonExistingGroupShouldReturnNoError()
        {
            //From documentation: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+ProtocolTests#AGuideToTheKafkaProtocol-OffsetFetchRequest
            //Note that if there is no offset associated with a topic-partition under that consumer group the broker does not set an error code
            //(since it is not really an error), but returns empty metadata and sets the offset field to -1.
            const int partitionId = 0;
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var request = new OffsetFetchRequest(Guid.NewGuid().ToString(), new TopicPartition(topicName, partitionId));
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    var conn = router.GetTopicBroker(topicName, partitionId);

                    var response = await conn.Connection.SendAsync(request, CancellationToken.None);
                    var topic = response.Topics.FirstOrDefault();

                    Assert.That(topic, Is.Not.Null);
                    Assert.That(topic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
                    Assert.That(topic.Offset, Is.EqualTo(-1));
                });
            }
        }

        [Test]
        public async Task OffsetCommitShouldStoreAndReturnSuccess()
        {
            const int partitionId = 0;
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    var conn = router.GetTopicBroker(topicName, partitionId);

                    // ensure the group exists
                    var groupId = TestConfig.GroupId();
                    var group = new GroupCoordinatorRequest(groupId);
                    var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
                    Assert.That(groupResponse, Is.Not.Null);
                    Assert.That(groupResponse.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

                    var commit = new OffsetCommitRequest(group.GroupId, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, 10, null) });
                    var response = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var topic = response.Topics.FirstOrDefault();

                    Assert.That(topic, Is.Not.Null);
                    Assert.That(topic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
                });
            }
        }

        [Test]
        public async Task OffsetCommitShouldStoreOffsetValue()
        {
            const int partitionId = 0;
            const long offset = 99;

            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    var conn = router.GetTopicBroker(topicName, partitionId);

                    // ensure the group exists
                    var groupId = TestConfig.GroupId();
                    var group = new GroupCoordinatorRequest(groupId);
                    var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
                    Assert.That(groupResponse, Is.Not.Null);
                    Assert.That(groupResponse.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

                    var commit = new OffsetCommitRequest(group.GroupId, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, offset, null) });
                    var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var commitTopic = commitResponse.Topics.SingleOrDefault();

                    Assert.That(commitTopic, Is.Not.Null);
                    Assert.That(commitTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

                    var fetch = new OffsetFetchRequest(groupId, new TopicPartition(topicName, partitionId));
                    var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
                    var fetchTopic = fetchResponse.Topics.SingleOrDefault();

                    Assert.That(fetchTopic, Is.Not.Null);
                    Assert.That(fetchTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
                    Assert.That(fetchTopic.Offset, Is.EqualTo(offset));
                });
            }
        }

        [Test]
        public async Task OffsetCommitShouldStoreMetadata()
        {
            const int partitionId = 0;
            const long offset = 101;
            const string metadata = "metadata";

            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var conn = await router.GetTopicBrokerAsync(topicName, partitionId, CancellationToken.None);

                    // ensure the group exists
                    var groupId = TestConfig.GroupId();
                    var group = new GroupCoordinatorRequest(groupId);
                    var groupResponse = await conn.Connection.SendAsync(group, CancellationToken.None);
                    Assert.That(groupResponse, Is.Not.Null);
                    Assert.That(groupResponse.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

                    var commit = new OffsetCommitRequest(group.GroupId, new []{ new OffsetCommitRequest.Topic(topicName, partitionId, offset, metadata) });
                    var commitResponse = await conn.Connection.SendAsync(commit, CancellationToken.None);
                    var commitTopic = commitResponse.Topics.SingleOrDefault();

                    Assert.That(commitTopic, Is.Not.Null);
                    Assert.That(commitTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));

                    var fetch = new OffsetFetchRequest(groupId, commitTopic);
                    var fetchResponse = await conn.Connection.SendAsync(fetch, CancellationToken.None);
                    var fetchTopic = fetchResponse.Topics.SingleOrDefault();

                    Assert.That(fetchTopic, Is.Not.Null);
                    Assert.That(fetchTopic.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
                    Assert.That(fetchTopic.Offset, Is.EqualTo(offset));
                    Assert.That(fetchTopic.MetaData, Is.EqualTo(metadata));
                });
            }
        }

        [Test]
        public async Task ConsumerMetadataRequestShouldReturnWithoutError()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var conn = await router.GetTopicBrokerAsync(topicName, 0, CancellationToken.None);

                    var groupId = TestConfig.GroupId();
                    var request = new GroupCoordinatorRequest(groupId);

                    var response = await conn.Connection.SendAsync(request, CancellationToken.None);

                    Assert.That(response, Is.Not.Null);
                    Assert.That(response.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
                });
            }
        }

        [Test]
        public async Task CanCreateAndDeleteTopics()
        {
            using (var router = new Router(_options)) {
                await router.TemporaryTopicAsync(async topicName => {
                    var response = await router.GetTopicMetadataAsync(topicName, CancellationToken.None);
                    Assert.That(response.ErrorCode, Is.EqualTo(ErrorResponseCode.None));
                });
            }
        }
    }
}