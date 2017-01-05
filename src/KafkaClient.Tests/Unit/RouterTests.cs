using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NSubstitute;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class RouterTests
    {
        private const string TestTopic = BrokerRouterProxy.TestTopic;
        private IConnection _connection;
        private IConnectionFactory _connectionFactory;

        [SetUp]
        public void Setup()
        {
            //setup mock IConnection
            _connection = Substitute.For<IConnection>();
            _connectionFactory = Substitute.For<IConnectionFactory>();
            _connectionFactory
                .Create(Arg.Is<Endpoint>(e => e.IP.Port == 1), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>())
                .Returns(_ => _connection);
            _connectionFactory
                .Resolve(Arg.Any<Uri>(), Arg.Any<ILog>())
                .Returns(_ => new Endpoint(_.Arg<Uri>(), new IPEndPoint(IPAddress.Parse("127.0.0.1"), _.Arg<Uri>().Port)));
        }

        [Test]
        public void BrokerRouterCanConstruct()
        {
            var result = new Router(new Uri("http://localhost:1"), _connectionFactory);

            Assert.That(result, Is.Not.Null);
        }

        [Test]
        public void BrokerRouterConstructorThrowsException()
        {
            Assert.Throws<ConnectionException>(() => new Router(new Uri("http://noaddress:1")));
        }

        [Test]
        public void BrokerRouterConstructorShouldIgnoreUnresolvableUriWhenAtLeastOneIsGood()
        {
            var result = new Router(new [] { new Uri("http://noaddress:1"), new Uri("http://localhost:1") });
        }

        [Test]
        public async Task BrokerRouterUsesFactoryToAddNewBrokers()
        {
            var router = new Router(new Uri("http://localhost:1"), _connectionFactory);

            _connection
                .SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers());
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var topics = router.GetTopicMetadata(TestTopic);
            _connectionFactory.Received()
                              .Create(Arg.Is<Endpoint>(e => e.IP.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>());
        }

        [Test]
        public async Task BrokerRouterUsesFactoryToAddNewBrokersFromGroups()
        {
            var router = new Router(new Uri("http://localhost:1"), _connectionFactory);

            _connection
                .SendAsync(Arg.Any<GroupCoordinatorRequest>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(_ => BrokerRouterProxy.CreateGroupCoordinatorResponse(1));
            await router.GetGroupBrokerAsync(TestTopic, CancellationToken.None);
            var broker = router.GetGroupBroker(TestTopic);
            _connectionFactory.Received()
                              .Create(Arg.Is<Endpoint>(e => e.IP.Port == 2), Arg.Any<IConnectionConfiguration>(), Arg.Any<ILog>());
        }

        #region Group Tests

        [Test]
        public async Task GetGroupShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var routerProxy = new BrokerRouterProxy();
            var routerProxy1 = routerProxy.Create();
            Assert.Throws<CachedMetadataException>(() => routerProxy1.GetGroupBroker("unknown"));
        }

        [Test]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFoundForGroup()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.GroupCoordinatorResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();
            await router.GetGroupBrokerAsync(TestTopic, CancellationToken.None);
            var result = router.GetGroupBroker(TestTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(1));
            Assert.That(routerProxy.Connection2.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServerForGroup()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.GroupCoordinatorResponseFunction = () => { throw new Exception("some error"); };
            routerProxy.Connection2.GroupCoordinatorResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();

            Assert.ThrowsAsync<CachedMetadataException>(async () => await router.GetGroupBrokerAsync(TestTopic, CancellationToken.None));

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(1));
            Assert.That(routerProxy.Connection2.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldReturnGroupFromCache()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetGroupBrokerAsync(TestTopic, CancellationToken.None);
            var result1 = router.GetGroupBroker(TestTopic);
            var result2 = router.GetGroupBroker(TestTopic);

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(1));
            Assert.That(result1.GroupId, Is.EqualTo(TestTopic));
            Assert.That(result2.GroupId, Is.EqualTo(TestTopic));
        }

        [Test]
        public async Task RefreshGroupMetadataShouldIgnoreCacheAndAlwaysCauseRequestAfterExpirationDate()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            TimeSpan cacheExpiration = TimeSpan.FromMilliseconds(100);
            await router.RefreshGroupMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(1));
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);//After cache is expired
            await router.RefreshGroupMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyRefreshGroupMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.RefreshGroupMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            x.Add(router.RefreshGroupMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyGetGroupMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.GetGroupBrokerAsync(TestTopic, CancellationToken.None));//do not debug
            x.Add(router.GetGroupBrokerAsync(TestTopic, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.GroupCoordinator), Is.EqualTo(1));
        }

        #endregion

        #region MetadataRequest Tests...

        [Test]
        public async Task BrokerRouteShouldCycleThroughEachBrokerUntilOneIsFound()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result = router.GetTopicMetadata(TestTopic);
            Assert.That(result, Is.Not.Null);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
            Assert.That(routerProxy.Connection2.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldThrowIfCycleCouldNotConnectToAnyServer()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.Connection1.MetadataResponseFunction = () => { throw new Exception("some error"); };
            routerProxy.Connection2.MetadataResponseFunction = () => { throw new Exception("some error"); };
            var router = routerProxy.Create();

            Assert.ThrowsAsync<ConnectionException>(async () => await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
            Assert.That(routerProxy.Connection2.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
        }

        [Test]
        public async Task BrokerRouteShouldReturnTopicFromCache()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result1 = router.GetTopicMetadata(TestTopic);
            var result2 = router.GetTopicMetadata(TestTopic);

            Assert.AreEqual(1, router.GetTopicMetadata().Count);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
            Assert.That(result1.TopicName, Is.EqualTo(TestTopic));
            Assert.That(result2.TopicName, Is.EqualTo(TestTopic));
        }

        [Test]
        public async Task BrokerRouteShouldThrowNoLeaderElectedForPartition()
        {
            var routerProxy = new BrokerRouterProxy {
                MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithNotEndToElectLeader
            };

            var router = routerProxy.Create();
            Assert.ThrowsAsync<CachedMetadataException>(async () => await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));
            Assert.AreEqual(0, router.GetTopicMetadata().Count);
        }

        [Test]
        public async Task BrokerRouteShouldReturnAllTopicsFromCache()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            var result1 = router.GetTopicMetadata();
            var result2 = router.GetTopicMetadata();

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
            Assert.That(result1.Count, Is.EqualTo(1));
            Assert.That(result1[0].TopicName, Is.EqualTo(TestTopic));
            Assert.That(result2.Count, Is.EqualTo(1));
            Assert.That(result2[0].TopicName, Is.EqualTo(TestTopic));
        }

        [Test]
        public async Task RefreshTopicMetadataShouldIgnoreCacheAndAlwaysCauseMetadataRequestAfterExpirationDate()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            TimeSpan cacheExpiration = TimeSpan.FromMilliseconds(100);
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);//After cache is expired
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(2));
        }

        [Test]
        public async Task RefreshAllTopicMetadataShouldAlwaysDoRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
            await router.RefreshTopicMetadataAsync(CancellationToken.None);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(2));
        }

        [Test]
        public async Task SelectBrokerRouteShouldChange()
        {
            var routerProxy = new BrokerRouterProxy();

            var router = routerProxy.Create();

            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);

            var router1 = router.GetTopicBroker(TestTopic, 0);

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);//After cache is expired
            routerProxy.MetadataResponse = BrokerRouterProxy.CreateMetadataResponseWithSingleBroker;
            await router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None);
            var router2 = router.GetTopicBroker(TestTopic, 0);

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(2));
            Assert.That(router1.Connection.Endpoint, Is.EqualTo(routerProxy.Connection1.Endpoint));
            Assert.That(router2.Connection.Endpoint, Is.EqualTo(routerProxy.Connection2.Endpoint));
            Assert.That(router1.Connection.Endpoint, Is.Not.EqualTo(router2.Connection.Endpoint));
        }

        [Test]
        public async Task SimultaneouslyRefreshTopicMetadataShouldNotGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            x.Add(router.RefreshTopicMetadataAsync(TestTopic, true, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(2));
        }

        [Test]
        public async Task SimultaneouslyGetTopicMetadataShouldGetDataFromCacheOnSameRequest()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();

            List<Task> x = new List<Task>();
            x.Add(router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));//do not debug
            x.Add(router.GetTopicMetadataAsync(TestTopic, CancellationToken.None));//do not debug
            await Task.WhenAll(x.ToArray());
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
        }

        #endregion MetadataRequest Tests...

        #region SelectBrokerRouteAsync Exact Tests...

        [Test]
        public async Task SelectExactPartitionShouldReturnRequestedPartition()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var p0 = router.GetTopicBroker(TestTopic, 0);
            var p1 = router.GetTopicBroker(TestTopic, 1);

            Assert.That(p0.PartitionId, Is.EqualTo(0));
            Assert.That(p1.PartitionId, Is.EqualTo(1));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenPartitionDoesNotExist()
        {
            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            Assert.Throws<CachedMetadataException>(() => router.GetTopicBroker(TestTopic, 3));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998

            Assert.Throws<CachedMetadataException>(() => routerProxy.Create().GetTopicBroker(TestTopic, 1));
        }

        [Test]
        public async Task SelectExactPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse = new MetadataResponse(topics: metadataResponse.Topics);

            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            Assert.Throws<CachedMetadataException>(() => router.GetTopicBroker(TestTopic, 1));
        }

        #endregion SelectBrokerRouteAsync Exact Tests...

        #region SelectBrokerRouteAsync Select Tests...

        [Test]
        [TestCase(null)]
        [TestCase("withkey")]
        public async Task SelectPartitionShouldUsePartitionSelector(string testCase)
        {
            var key = testCase.ToIntSizedBytes();
            var routerProxy = new BrokerRouterProxy();

            var partitionSelector = Substitute.For<IPartitionSelector>();
            partitionSelector
                .Select(Arg.Any<MetadataResponse.Topic>(), key)
                .Returns(new MetadataResponse.Partition(0, 0, ErrorResponseCode.None, new[] { 1 }, new[] { 1 }));

            routerProxy.PartitionSelector = partitionSelector;
            var router = routerProxy.Create();
            await router.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            var result = router.GetTopicBroker(TestTopic, key);

            partitionSelector.Received().Select(Arg.Is<MetadataResponse.Topic>(x => x.TopicName == TestTopic), key);
        }

        [Test]
        public async Task SelectPartitionShouldThrowWhenTopicsCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse.Topics.Clear();

            var routerProxy = new BrokerRouterProxy();
#pragma warning disable 1998
            routerProxy.Connection1.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998

            Assert.Throws<CachedMetadataException>(() => routerProxy.Create().GetTopicBroker(TestTopic, new byte[] {}));
        }

        [Test]
        public async Task SelectPartitionShouldThrowWhenBrokerCollectionIsEmpty()
        {
            var metadataResponse = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
            metadataResponse = new MetadataResponse(topics: metadataResponse.Topics);

            var routerProxy = new BrokerRouterProxy();
            var router = routerProxy.Connection1;
#pragma warning disable 1998
            routerProxy.Connection1.MetadataResponseFunction = async () => metadataResponse;
#pragma warning restore 1998
            var routerProxy1 = routerProxy.Create();
            await routerProxy1.GetTopicMetadataAsync(TestTopic, CancellationToken.None);
            Assert.Throws<CachedMetadataException>(() => routerProxy1.GetTopicBroker(TestTopic, new byte[] {}));
        }

        #endregion SelectBrokerRouteAsync Select Tests...
    }
}