using System;
using System.Collections.Generic;
using System.Linq;
using KafkaClient.Assignment;
using KafkaClient.Common;
using KafkaClient.Protocol;
using NUnit.Framework;
using NUnit.Framework.Internal;

namespace KafkaClient.Tests.Unit
{
    /// <summary>
    /// From http://kafka.apache.org/protocol.html#protocol_types
    /// The protocol is built out of the following primitive types.
    ///
    /// Fixed Width Primitives:
    /// int8, int16, int32, int64 - Signed integers with the given precision (in bits) stored in big endian order.
    ///
    /// Variable Length Primitives:
    /// bytes, string - These types consist of a signed integer giving a length N followed by N bytes of content. 
    /// A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
    ///
    /// Arrays:
    /// This is a notation for handling repeated structures. These will always be encoded as an int32 size containing 
    /// the length N followed by N repetitions of the structure which can itself be made up of other primitive types. 
    /// In the BNF grammars below we will show an array of a structure foo as [foo].
    /// 
    /// Message formats are from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-CommonRequestandResponseStructure
    /// 
    /// RequestOrResponse => Size (RequestMessage | ResponseMessage)
    ///  Size => int32    : The Size field gives the size of the subsequent request or response message in bytes. 
    ///                     The client can read requests by first reading this 4 byte size as an integer N, and 
    ///                     then reading and parsing the subsequent N bytes of the request.
    /// 
    /// Request Header => api_key api_version correlation_id client_id 
    ///  api_key => INT16             -- The id of the request type.
    ///  api_version => INT16         -- The version of the API.
    ///  correlation_id => INT32      -- A user-supplied integer value that will be passed back with the response.
    ///  client_id => NULLABLE_STRING -- A user specified identifier for the client making the request.
    /// 
    /// Response Header => correlation_id 
    ///  correlation_id => INT32      -- The user-supplied value passed in with the request
    /// </summary>
    [TestFixture]
    public class ProtocolByteTests
    {
        private readonly Randomizer _randomizer = new Randomizer();

        [Test]
        public void ProduceRequest(
            [Values(0, 1, 2)] short version,
            [Values(0, 2, -1)] short acks, 
            [Values(0, 1000)] int timeoutMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(3)] int messagesPerSet)
        {
            var payloads = new List<ProduceRequest.Payload>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partition = 1 + t%totalPartitions;
                payloads.Add(new ProduceRequest.Payload(topic + t, partition, GenerateMessages(messagesPerSet, (byte) (version >= 2 ? 1 : 0), partition)));
            }
            var request = new ProduceRequest(payloads, TimeSpan.FromMilliseconds(timeoutMilliseconds), acks);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void ProduceResponse(
            [Values(0, 1, 2)] short version,
            [Values(-1, 0, 10000000)] long timestampMilliseconds, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.None,
                ErrorResponseCode.CorruptMessage
            )] ErrorResponseCode errorCode,
            [Values(0, 100000)] int throttleTime)
        {
            var topics = new List<ProduceResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new ProduceResponse.Topic(topicName + t, t % totalPartitions, errorCode, _randomizer.Next(), version >= 2 ? DateTimeOffset.FromUnixTimeMilliseconds(timestampMilliseconds) : (DateTimeOffset?)null));
            }
            var response = new ProduceResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void FetchRequest(
            [Values(0, 1, 2, 3)] short version,
            [Values(0, 100)] int maxWaitMilliseconds, 
            [Values(0, 64000)] int minBytes, 
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(25600000)] int maxBytes)
        {
            var fetches = new List<FetchRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                fetches.Add(new FetchRequest.Topic(topic + t, t % totalPartitions, _randomizer.Next(0, int.MaxValue), maxBytes));
            }
            var request = new FetchRequest(fetches, TimeSpan.FromMilliseconds(maxWaitMilliseconds), minBytes, version >= 3 ? maxBytes / _randomizer.Next(1, maxBytes) : 0);
            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void FetchResponse(
            [Values(0, 1, 2, 3)] short version,
            [Values(0, 1234)] int throttleTime,
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.None,
                ErrorResponseCode.OffsetOutOfRange
            )] ErrorResponseCode errorCode, 
            [Values(3)] int messagesPerSet
            )
        {
            var topics = new List<FetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                var messages = GenerateMessages(messagesPerSet, (byte) (version >= 2 ? 1 : 0), partitionId);
                topics.Add(new FetchResponse.Topic(topicName + t, partitionId, _randomizer.Next(), errorCode, messages));
            }
            var response = new FetchResponse(topics, version >= 1 ? TimeSpan.FromMilliseconds(throttleTime) : (TimeSpan?)null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void OffsetsRequest(
            [Values(0, 1)] short version,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(1, 5)] int totalPartitions, 
            [Values(-2, -1, 123456, 10000000)] long time,
            [Values(1, 10)] int maxOffsets)
        {
            var topics = new List<OffsetRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var offset = new OffsetRequest.Topic(topic + t, t % totalPartitions, time, version == 0 ? maxOffsets : 1);
                topics.Add(offset);
            }
            var request = new OffsetRequest(topics);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void OffsetsResponse(
            [Values(0, 1)] short version,
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName, 
            [Values(1, 10)] int topicsPerRequest, 
            [Values(5)] int totalPartitions, 
            [Values(
                ErrorResponseCode.UnknownTopicOrPartition,
                ErrorResponseCode.NotLeaderForPartition,
                ErrorResponseCode.Unknown
            )] ErrorResponseCode errorCode, 
            [Values(1, 5)] int offsetsPerPartition)
        {
            var topics = new List<OffsetResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitionId = t % totalPartitions;
                for (var o = 0; o < offsetsPerPartition; o++) {
                    topics.Add(new OffsetResponse.Topic(topicName + t, partitionId, errorCode, _randomizer.Next(-1, int.MaxValue), version >= 1 ? (DateTimeOffset?)DateTimeOffset.UtcNow : null));
                }
            }
            var response = new OffsetResponse(topics);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void MetadataRequest(
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(0, 1, 10)] int topicsPerRequest)
        {
            var topics = new List<string>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(topic + t);
            }
            var request = new MetadataRequest(topics);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void MetadataResponse(
            [Values(0, 1, 2)] short version,
            [Values(1, 15)] int brokersPerRequest,
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.UnknownTopicOrPartition
             )] ErrorResponseCode errorCode)
        {
            var brokers = new List<KafkaClient.Protocol.Broker>();
            for (var b = 0; b < brokersPerRequest; b++) {
                string rack = null;
                if (version >= 1) {
                    rack = "Rack" + b;
                }
                brokers.Add(new KafkaClient.Protocol.Broker(b, "broker-" + b, 9092 + b, rack));
            }
            var topics = new List<MetadataResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var partitions = new List<MetadataResponse.Partition>();
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var leader = _randomizer.Next(0, brokersPerRequest - 1);
                    var replica = 0;
                    var replicas = _randomizer.Next(0, brokersPerRequest - 1).Repeat(() => replica++);
                    var isr = 0;
                    var isrs = _randomizer.Next(0, replica).Repeat(() => isr++);
                    partitions.Add(new MetadataResponse.Partition(partitionId, leader, errorCode, replicas, isrs));
                }
                topics.Add(new MetadataResponse.Topic(topicName + t, errorCode, partitions, version >= 1 ? topicsPerRequest%2 == 0 : (bool?)null));
            }
            var response = new MetadataResponse(brokers, topics, version >= 1 ? brokersPerRequest : (int?)null, version >= 2 ? $"cluster-{version}" : null);

            response.AssertCanEncodeDecodeResponse(version);
        }

        [Test]
        public void OffsetCommitRequest(
            [Values(0, 1, 2)] short version,
            [Values("group1", "group2")] string groupId,
            [Values(0, 5)] int generation,
            [Values(-1, 20000)] int retentionTime,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(5)] int maxPartitions,
            [Values(10)] int maxOffsets,
            [Values(null, "something useful for the client")] string metadata)
        {
            var timestamp = retentionTime;
            var offsetCommits = new List<OffsetCommitRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                offsetCommits.Add(new OffsetCommitRequest.Topic(
                                      topic + t,
                                      t%maxPartitions,
                                      _randomizer.Next(0, int.MaxValue),
                                      metadata,
                                      version == 1 ? timestamp : (long?)null));
            }
            var request = new OffsetCommitRequest(
                groupId,
                offsetCommits,
                version >= 1 ? "member" + generation : null,
                version >= 1 ? generation : 0,
                version >= 2 && retentionTime >= 0 ? (TimeSpan?) TimeSpan.FromMilliseconds(retentionTime) : null);

            request.AssertCanEncodeDecodeRequest(version);
        }

        [Test]
        public void OffsetCommitResponse(
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode)
        {
            var topics = new List<TopicResponse>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    topics.Add(new TopicResponse(topicName + t, partitionId, errorCode));
                }
            }
            var response = new OffsetCommitResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void OffsetFetchRequest(
            [Values("group1", "group2")] string groupId,
            [Values("test", "a really long name, with spaces and punctuation!")] string topic,
            [Values(1, 10)] int topicsPerRequest,
            [Values(5)] int maxPartitions)
        {
            var topics = new List<TopicPartition>();
            for (var t = 0; t < topicsPerRequest; t++) {
                topics.Add(new TopicPartition(topic + t, t % maxPartitions));
            }
            var request = new OffsetFetchRequest(groupId, topics);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void OffsetFetchResponse(
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.UnknownTopicOrPartition,
                 ErrorResponseCode.GroupLoadInProgress,
                 ErrorResponseCode.NotCoordinatorForGroup,
                 ErrorResponseCode.IllegalGeneration,
                 ErrorResponseCode.UnknownMemberId,
                 ErrorResponseCode.TopicAuthorizationFailed,
                 ErrorResponseCode.GroupAuthorizationFailed
             )] ErrorResponseCode errorCode)
        {
            var topics = new List<OffsetFetchResponse.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var offset = (long)_randomizer.Next(int.MinValue, int.MaxValue);
                    topics.Add(new OffsetFetchResponse.Topic(topicName + t, partitionId, errorCode, offset, offset >= 0 ? topicName : string.Empty));
                }
            }
            var response = new OffsetFetchResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void GroupCoordinatorRequest([Values("group1", "group2")] string groupId)
        {
            var request = new GroupCoordinatorRequest(groupId);
            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void GroupCoordinatorResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.GroupCoordinatorNotAvailable,
                 ErrorResponseCode.GroupAuthorizationFailed
             )] ErrorResponseCode errorCode,
            [Values(0, 1)] int coordinatorId
            )
        {
            var response = new GroupCoordinatorResponse(errorCode, coordinatorId, "broker-" + coordinatorId, 9092 + coordinatorId);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void ApiVersionsRequest()
        {
            var request = new ApiVersionsRequest();
            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void ApiVersionsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.BrokerNotAvailable
             )] ErrorResponseCode errorCode
            )
        {
            var supported = new List<ApiVersionsResponse.VersionSupport>();
            for (short apiKey = 0; apiKey <= 18; apiKey++) {
                supported.Add(new ApiVersionsResponse.VersionSupport((ApiKeyRequestType)apiKey, 0, (short)_randomizer.Next(0, 2)));
            }
            var response = new ApiVersionsResponse(errorCode, supported);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void JoinGroupRequest(
            [Values(0, 1)] short version,
            [Values("test", "a groupId")] string groupId, 
            [Values(1, 20000)] int sessionTimeout,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer", "other")] string protocolType, 
            [Values(1, 10)] int protocolsPerRequest)
        {
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++) {
                var bytes = new byte[protocolsPerRequest*100];
                _randomizer.NextBytes(bytes);
                protocols.Add(new JoinGroupRequest.GroupProtocol(new ByteTypeMetadata("known", bytes)));
            }
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, protocolType, protocols, version >= 1 ? (TimeSpan?)TimeSpan.FromMilliseconds(sessionTimeout * 2) : null);

            request.AssertCanEncodeDecodeRequest(version, new ByteMembershipEncoder(protocolType));
        }

        [Test]
        public void JoinGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values(0, 1, 20000)] int generationId,
            [Values("consumer", "other")] string protocol, 
            [Values("test", "a groupId")] string leaderId, 
            [Values("", "an existing member")] string memberId, 
            [Values(1, 10)] int memberCount)
        {
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++) {
                var bytes = new byte[memberCount*100];
                _randomizer.NextBytes(bytes);
                members.Add(new JoinGroupResponse.Member(memberId + m, new ByteTypeMetadata("known", bytes)));
            }
            var response = new JoinGroupResponse(errorCode, generationId, "known", leaderId, memberId, members);

            response.AssertCanEncodeDecodeResponse(0, new ByteMembershipEncoder(protocol));
        }

        [Test]
        public void JoinConsumerGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(1, 20000)] int sessionTimeout,
            [Values("", "an existing member")] string memberId, 
            [Values("mine", "yours")] string protocol, 
            [Values(1, 10)] int protocolsPerRequest)
        {
            var encoder = new ConsumerEncoder();
            var protocols = new List<JoinGroupRequest.GroupProtocol>();
            for (var p = 0; p < protocolsPerRequest; p++) {
                var userData = new byte[protocolsPerRequest*100];
                _randomizer.NextBytes(userData);
                var metadata = new ConsumerProtocolMetadata(new []{ groupId, memberId, protocol }, protocol + p, userData, 0);
                protocols.Add(new JoinGroupRequest.GroupProtocol(metadata));
            }
            var request = new JoinGroupRequest(groupId, TimeSpan.FromMilliseconds(sessionTimeout), memberId, ConsumerEncoder.Protocol, protocols);

            request.AssertCanEncodeDecodeRequest(0, encoder);
        }

        [Test]
        public void JoinConsumerGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values(0, 1, 20000)] int generationId,
            [Values("consumer")] string protocol, 
            [Values("test", "a groupId")] string leaderId, 
            [Values("", "an existing member")] string memberId, 
            [Values(1, 10)] int memberCount)
        {
            var encoder = new ConsumerEncoder();
            var members = new List<JoinGroupResponse.Member>();
            for (var m = 0; m < memberCount; m++) {
                var userData = new byte[memberCount*100];
                _randomizer.NextBytes(userData);
                var metadata = new ConsumerProtocolMetadata(new []{ protocol, memberId, leaderId }, protocol, userData, 0);
                members.Add(new JoinGroupResponse.Member(memberId + m, metadata));
            }
            var response = new JoinGroupResponse(errorCode, generationId, protocol, leaderId, memberId, members);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void HeartbeatRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId)
        {
            var request = new HeartbeatRequest(groupId, generationId, memberId);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void HeartbeatResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode)
        {
            var response = new HeartbeatResponse(errorCode);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void LeaveGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values("", "an existing member")] string memberId)
        {
            var request = new LeaveGroupRequest(groupId, memberId);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void LeaveGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode)
        {
            var response = new LeaveGroupResponse(errorCode);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void SyncGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer", "other")] string protocolType, 
            [Values(1, 10)] int assignmentsPerRequest)
        {
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var bytes = new byte[assignmentsPerRequest*100];
                _randomizer.NextBytes(bytes);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, new ByteTypeAssignment(bytes)));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);

            request.AssertCanEncodeDecodeRequest(0, new ByteMembershipEncoder(protocolType));
        }

        [Test]
        public void SyncGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode)
        {
            var bytes = new byte[1000];
            _randomizer.NextBytes(bytes);
            var response = new SyncGroupResponse(errorCode, new ByteTypeAssignment(bytes));

            response.AssertCanEncodeDecodeResponse(0, new ByteMembershipEncoder("protocolType"));
        }

        [Test]
        public void SyncConsumerGroupRequest(
            [Values("test", "a groupId")] string groupId, 
            [Values(0, 1, 20000)] int generationId,
            [Values("", "an existing member")] string memberId, 
            [Values("consumer")] string protocolType, 
            [Values(1, 10)] int assignmentsPerRequest)
        {
            var encoder = new ConsumerEncoder();
            var assignments = new List<SyncGroupRequest.GroupAssignment>();
            for (var a = 0; a < assignmentsPerRequest; a++) {
                var topics = new List<TopicPartition>();
                for (var t = 0; t < assignmentsPerRequest; t++) {
                    topics.Add(new TopicPartition(groupId + t, t));
                }
                var userData = new byte[assignmentsPerRequest*100];
                _randomizer.NextBytes(userData);
                var assignment = new ConsumerMemberAssignment(topics, userData, 0);
                assignments.Add(new SyncGroupRequest.GroupAssignment(protocolType + a, assignment));
            }
            var request = new SyncGroupRequest(groupId, generationId, memberId, assignments);

            request.AssertCanEncodeDecodeRequest(0, encoder);
        }

        [Test]
        public void SyncConsumerGroupResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values(1, 10)] int memberCount)
        {
            var encoder = new ConsumerEncoder();
            var topics = new List<TopicPartition>();
            for (var t = 0; t < memberCount; t++) {
                topics.Add(new TopicPartition("topic foo" + t, t));
            }
            var userData = new byte[memberCount*100];
            _randomizer.NextBytes(userData);
            var assignment = new ConsumerMemberAssignment(topics, userData, 0);
            var response = new SyncGroupResponse(errorCode, assignment);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void DescribeGroupsRequest(
            [Values("test", "a groupId")] string groupId, 
            [Range(1, 10)] int count)
        {
            var groups = new string[count];
            for (var g = 0; g < count; g++) {
                groups[g] = groupId + g;
            }
            var request = new DescribeGroupsRequest(groups);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void DescribeGroupsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Range(2, 3)] int count,
            [Values(KafkaClient.Protocol.DescribeGroupsResponse.Group.States.Stable, KafkaClient.Protocol.DescribeGroupsResponse.Group.States.Dead)] string state, 
            [Values("consumer", "unknown")] string protocolType,
            [Values("good", "bad", "ugly")] string protocol)
        {
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var metadata = new byte[count*100];
                    var assignment = new byte[count*10];
                    _randomizer.NextBytes(metadata);
                    _randomizer.NextBytes(assignment);

                    members.Add(new DescribeGroupsResponse.Member("member" + m, "client" + m, "host-" + m, new ByteTypeMetadata(protocol, metadata), new ByteTypeAssignment(assignment)));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var response = new DescribeGroupsResponse(groups);

            response.AssertCanEncodeDecodeResponse(0, new ByteMembershipEncoder(protocolType));
        }

        [Test]
        public void DescribeConsumerGroupsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Range(2, 3)] int count,
            [Values(KafkaClient.Protocol.DescribeGroupsResponse.Group.States.Stable, Protocol.DescribeGroupsResponse.Group.States.AwaitingSync)] string state, 
            [Values("consumer")] string protocolType,
            [Values("good", "bad", "ugly")] string protocol)
        {
            var encoder = new ConsumerEncoder();
            var groups = new DescribeGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                var members = new List<DescribeGroupsResponse.Member>();
                for (var m = 0; m < count; m++) {
                    var memberId = "member" + m;
                    var userData = new byte[count*100];
                    _randomizer.NextBytes(userData);
                    var metadata = new ConsumerProtocolMetadata(new []{ protocol, memberId, memberId }, protocol, userData, 0);

                    var topics = new List<TopicPartition>();
                    for (var t = 0; t < count; t++) {
                        topics.Add(new TopicPartition("topic foo" + t, t));
                    }
                    var assignment = new ConsumerMemberAssignment(topics, userData, 0);

                    members.Add(new DescribeGroupsResponse.Member(memberId, "client" + m, "host-" + m, metadata, assignment));
                }
                groups[g] = new DescribeGroupsResponse.Group(errorCode, groupId + g, state, protocolType, protocol, members);
            }
            var response = new DescribeGroupsResponse(groups);

            response.AssertCanEncodeDecodeResponse(0, encoder);
        }

        [Test]
        public void ListGroupsRequest()
        {
            var request = new ListGroupsRequest();
            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void ListGroupsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Values("test", "a groupId")] string groupId, 
            [Range(2, 3)] int count,
            [Values("consumer")] string protocolType)
        {
            var groups = new ListGroupsResponse.Group[count];
            for (var g = 0; g < count; g++) {
                groups[g] = new ListGroupsResponse.Group(groupId + g, protocolType);
            }
            var response = new ListGroupsResponse(errorCode, groups);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void SaslHandshakeRequest(
            [Values("EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER")] string mechanism)
        {
            var request = new SaslHandshakeRequest(mechanism);

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void SaslHandshakeResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.OffsetMetadataTooLarge
             )] ErrorResponseCode errorCode,
            [Range(1, 11)] int count)
        {
            var mechanisms = new[] { "EXTERNAL", "ANONYMOUS", "PLAIN", "OTP", "SKEY", "CRAM-MD5", "DIGEST-MD5", "SCRAM", "NTLM", "GSSAPI", "OAUTHBEARER" };
            var response = new SaslHandshakeResponse(errorCode, mechanisms.Take(count));

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void DeleteTopicsRequest(
            [Values("test", "anotherNameForATopic")] string topicName, 
            [Range(2, 3)] int count,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new string[count];
            for (var t = 0; t < count; t++) {
                topics[t] = topicName + t;
            }
            var request = new DeleteTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void DeleteTopicsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.NotController
             )] ErrorResponseCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName, 
            [Range(1, 11)] int count)
        {
            var topics = new TopicsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicsResponse.Topic(topicName + t, errorCode);
            }
            var response = new DeleteTopicsResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        [Test]
        public void CreateTopicsRequest(
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(1, 3)] short replicationFactor,
            [Values(0, 3)] int configCount,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new List<CreateTopicsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var configs = new Dictionary<string, string>();
                for (var c = 0; c < configCount; c++) {
                    configs["config-" + c] = Guid.NewGuid().ToString("N");
                }
                if (configs.Count == 0 && _randomizer.NextBool()) {
                    configs = null;
                }
                topics.Add(new CreateTopicsRequest.Topic(topicName + t, partitionsPerTopic, replicationFactor, configs));
            }
            var request = new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void CreateTopicsExplicitRequest(
            [Values("test", "a really long name, with spaces and punctuation!")] string topicName,
            [Values(1, 10)] int topicsPerRequest,
            [Values(1, 5)] int partitionsPerTopic,
            [Values(1, 3)] short replicationFactor,
            [Values(0, 3)] int configCount,
            [Values(0, 1, 20000)] int timeoutMilliseconds)
        {
            var topics = new List<CreateTopicsRequest.Topic>();
            for (var t = 0; t < topicsPerRequest; t++) {
                var configs = new Dictionary<string, string>();
                for (var c = 0; c < configCount; c++) {
                    configs["config-" + c] = Guid.NewGuid().ToString("N");
                }
                if (configs.Count == 0 && _randomizer.NextBool()) {
                    configs = null;
                }

                var assignments = new List<CreateTopicsRequest.ReplicaAssignment>();
                for (var partitionId = 0; partitionId < partitionsPerTopic; partitionId++) {
                    var replica = 0;
                    var replicas = _randomizer.Next(0, replicationFactor - 1).Repeat(() => replica++);
                    assignments.Add(new CreateTopicsRequest.ReplicaAssignment(partitionId, replicas));
                }
                topics.Add(new CreateTopicsRequest.Topic(topicName + t, assignments, configs));
            }
            var request = new CreateTopicsRequest(topics, TimeSpan.FromMilliseconds(timeoutMilliseconds));

            request.AssertCanEncodeDecodeRequest(0);
        }

        [Test]
        public void CreateTopicsResponse(
            [Values(
                 ErrorResponseCode.None,
                 ErrorResponseCode.InvalidTopic,
                ErrorResponseCode.InvalidPartitions
             )] ErrorResponseCode errorCode,
            [Values("test", "anotherNameForATopic")] string topicName, 
            [Range(1, 11)] int count)
        {
            var topics = new TopicsResponse.Topic[count];
            for (var t = 0; t < count; t++) {
                topics[t] = new TopicsResponse.Topic(topicName + t, errorCode);
            }
            var response = new CreateTopicsResponse(topics);

            response.AssertCanEncodeDecodeResponse(0);
        }

        private IEnumerable<Message> GenerateMessages(int count, byte version, int partition = 0)
        {
            var messages = new List<Message>();
            for (var m = 0; m < count; m++) {
                var key = m > 0 ? new byte[8] : null;
                var value = new byte[8*(m + 1)];
                if (key != null) {
                    _randomizer.NextBytes(key);
                }
                _randomizer.NextBytes(value);

                messages.Add(new Message(value, 0, partitionId: partition, version: version, key: key, timestamp: version > 0 ? DateTimeOffset.UtcNow : (DateTimeOffset?)null));
            }
            return messages;
        }
    }
}