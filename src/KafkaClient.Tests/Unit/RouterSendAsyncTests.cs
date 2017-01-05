using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class RouterSendAsyncTests
    {
        private const int PartitionId = 0;

        [TestCase(ErrorResponseCode.NotLeaderForPartition)]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        [TestCase(ErrorResponseCode.GroupCoordinatorNotAvailable)]
        [TestCase(ErrorResponseCode.UnknownTopicOrPartition)]
        [Test]
        public async Task ShouldTryToRefreshMataDataIfCanRecoverByRefreshMetadata(ErrorResponseCode code)
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.CacheExpiration = new TimeSpan(10);
            var router = routerProxy.Create();

            routerProxy.Connection1.FetchResponseFunction = FailedInFirstMessageError(code, routerProxy.CacheExpiration);
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;

            await router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, PartitionId, CancellationToken.None);

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(2));
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Fetch), Is.EqualTo(2));
        }

        [Test]
        [TestCase(typeof(ConnectionException))]
        [TestCase(typeof(FetchOutOfRangeException))]
        [TestCase(typeof(CachedMetadataException))]
        public async Task ShouldTryToRefreshMataDataIfOnExceptions(Type exceptionType)
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.CacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            routerProxy.Connection1.FetchResponseFunction = FailedInFirstMessageException(exceptionType, routerProxy.CacheExpiration);
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;

            await router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, PartitionId, CancellationToken.None);

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(2));
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Fetch), Is.EqualTo(2));
        }

        [TestCase(typeof(Exception))]
        [TestCase(typeof(RequestException))]
        public async Task SendProtocolRequestShouldThrowException(Type exceptionType)
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.CacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            routerProxy.Connection1.FetchResponseFunction = FailedInFirstMessageException(exceptionType, routerProxy.CacheExpiration);
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            Assert.ThrowsAsync(exceptionType, async () => await router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, PartitionId, CancellationToken.None));
        }

        [Test]
        [TestCase(ErrorResponseCode.InvalidFetchSize)]
        [TestCase(ErrorResponseCode.MessageTooLarge)]
        [TestCase(ErrorResponseCode.OffsetMetadataTooLarge)]
        [TestCase(ErrorResponseCode.OffsetOutOfRange)]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.StaleControllerEpoch)]
        [TestCase(ErrorResponseCode.ReplicaNotAvailable)]
        public async Task SendProtocolRequestShouldNotTryToRefreshMataDataIfCanNotRecoverByRefreshMetadata(
            ErrorResponseCode code)
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.CacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            routerProxy.Connection1.FetchResponseFunction = FailedInFirstMessageError(code, routerProxy.CacheExpiration);
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            Assert.ThrowsAsync<RequestException>(async () => await router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, PartitionId, CancellationToken.None));
        }

        [Test]
        public async Task ShouldUpdateMetadataOnce()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.CacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            routerProxy.Connection1.FetchResponseFunction = ShouldReturnValidMessage;
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, PartitionId, CancellationToken.None);
            }
            await Task.Delay(routerProxy.CacheExpiration);
            await Task.Delay(1);
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(new FetchRequest(), BrokerRouterProxy.TestTopic, PartitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Fetch), Is.EqualTo(numberOfCall));
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1));
        }

        [Test]
        public async Task ShouldRecoverUpdateMetadataForNewTopic()
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.CacheExpiration = TimeSpan.FromMilliseconds(10);
            var router = routerProxy.Create();

            var fetchRequest = new FetchRequest();

            routerProxy.Connection1.FetchResponseFunction = ShouldReturnValidMessage;
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            int numberOfCall = 1000;
            Task[] tasks = new Task[numberOfCall];
            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, BrokerRouterProxy.TestTopic, PartitionId, CancellationToken.None);
            }

            routerProxy.Connection1.MetadataResponseFunction = async () =>
            {
                var response = await BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers();
                return new MetadataResponse(response.Brokers, response.Topics.Select(t => new MetadataResponse.Topic("test2", t.ErrorCode, t.Partitions)));
            };

            for (int i = 0; i < numberOfCall / 2; i++)
            {
                tasks[i + numberOfCall / 2] = router.SendAsync(fetchRequest, "test2", PartitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Fetch), Is.EqualTo(numberOfCall));
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(2));
        }

        [Test]
        public async Task ShouldRecoverFromFailureByUpdateMetadataOnce() //Do not debug this test !!
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.CacheExpiration = TimeSpan.FromMilliseconds(1000);
            var router = routerProxy.Create();

            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            int numberOfCall = 100;
            long numberOfErrorSend = 0;
            TaskCompletionSource<int> x = new TaskCompletionSource<int>();
            Func<Task<FetchResponse>> ShouldReturnNotLeaderForPartitionAndThenNoError = async () =>
            {
                var log = TestConfig.Log;
                log.Debug(() => LogEvent.Create("FetchResponse Start "));
                if (!x.Task.IsCompleted)
                {
                    if (Interlocked.Increment(ref numberOfErrorSend) == numberOfCall)
                    {
                        await Task.Delay(routerProxy.CacheExpiration);
                        await Task.Delay(1);
                        x.TrySetResult(1);
                        log.Debug(() => LogEvent.Create("all is complete "));
                    }

                    await x.Task;
                    log.Debug(() => LogEvent.Create("SocketException "));
                    throw new ConnectionException("");
                }
                log.Debug(() => LogEvent.Create("Completed "));

                return new FetchResponse();
            };

            routerProxy.Connection1.FetchResponseFunction = ShouldReturnNotLeaderForPartitionAndThenNoError;
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;

            Task[] tasks = new Task[numberOfCall];

            for (int i = 0; i < numberOfCall; i++)
            {
                tasks[i] = router.SendAsync(fetchRequest, BrokerRouterProxy.TestTopic, partitionId, CancellationToken.None);
            }

            await Task.WhenAll(tasks);
            Assert.That(numberOfErrorSend, Is.GreaterThan(1), "numberOfErrorSend");
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Fetch), Is.EqualTo(numberOfCall + numberOfErrorSend),
                "RequestCallCount(ApiKeyRequestType.Fetch)");
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(2), "RequestCallCount(ApiKeyRequestType.Metadata)");
        }

        [Test]
        public async Task ShouldRecoverFromConnectionExceptionByUpdateMetadataOnceFullScenario() //Do not debug this test !!
        {
            await ShouldRecoverByUpdateMetadataOnceFullScenario(
                FailedInFirstMessageException(typeof(ConnectionException), TimeSpan.Zero));
        }

        [Test]
        public async Task ShouldRecoverFromFetchErrorByUpdateMetadataOnceFullScenario1()
        {
            await ShouldRecoverByUpdateMetadataOnceFullScenario(
                FailedInFirstMessageError(ErrorResponseCode.LeaderNotAvailable, TimeSpan.Zero));
        }

        /// <summary>
        /// Do not debug this test !!
        /// </summary>
        private async Task ShouldRecoverByUpdateMetadataOnceFullScenario(Func<Task<FetchResponse>> fetchResponse) 
        {
            var routerProxy = new BrokerRouterProxy();
            routerProxy.CacheExpiration = TimeSpan.FromMilliseconds(0);
            var router = routerProxy.Create();
            int partitionId = 0;
            var fetchRequest = new FetchRequest();

            CreateSuccessfulSendMock(routerProxy);

            //Send Successful Message
            await router.SendAsync(fetchRequest, BrokerRouterProxy.TestTopic, partitionId, CancellationToken.None);

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Fetch), Is.EqualTo(1), "RequestCallCount(ApiKeyRequestType.Fetch)");
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1), "RequestCallCount(ApiKeyRequestType.Metadata)");
            Assert.That(routerProxy.Connection2.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(0), "RequestCallCount(ApiKeyRequestType.Metadata)");

            routerProxy.Connection1.FetchResponseFunction = fetchResponse;
            //triger to update metadata
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetaResponseWithException;
            routerProxy.Connection2.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithSingleBroker;

            //Reset variables
            routerProxy.Connection1.ResetRequestCallCount(ApiKeyRequestType.Fetch);
            routerProxy.Connection2.ResetRequestCallCount(ApiKeyRequestType.Fetch);
            routerProxy.Connection1.ResetRequestCallCount(ApiKeyRequestType.Metadata);
            routerProxy.Connection2.ResetRequestCallCount(ApiKeyRequestType.Metadata);

            //Send Successful Message that was recover from exception
            await router.SendAsync(fetchRequest, BrokerRouterProxy.TestTopic, partitionId, CancellationToken.None);

            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Fetch), Is.EqualTo(1), "RequestCallCount(ApiKeyRequestType.Fetch)");
            Assert.That(routerProxy.Connection1.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1), "RequestCallCount(ApiKeyRequestType.Metadata)");

            Assert.That(routerProxy.Connection2.RequestCallCount(ApiKeyRequestType.Fetch), Is.EqualTo(1), "RequestCallCount(ApiKeyRequestType.Fetch)");
            Assert.That(routerProxy.Connection2.RequestCallCount(ApiKeyRequestType.Metadata), Is.EqualTo(1), "RequestCallCount(ApiKeyRequestType.Metadata)");
        }


        private static Func<Task<FetchResponse>> FailedInFirstMessageError(ErrorResponseCode errorResponseCode, TimeSpan delay)
        {
            bool firstTime = true;
            Func<Task<FetchResponse>> result = async () => {
                if (firstTime) {
                    await Task.Delay(delay);
                    await Task.Delay(1);
                    firstTime = false;
                    return new FetchResponse(new []{ new FetchResponse.Topic("foo", 1, 0, errorResponseCode)});
                }
                return new FetchResponse();
            };
            return result;
        }

        private Func<Task<FetchResponse>> FailedInFirstMessageException(Type exceptionType, TimeSpan delay)
        {
            bool firstTime = true;
            Func<Task<FetchResponse>> result = async () =>
            {
                if (firstTime)
                {
                    await Task.Delay(delay);
                    await Task.Delay(1);
                    firstTime = false;
                    if (exceptionType == typeof(ConnectionException))
                    {
                        throw new ConnectionException("");
                    }
                    object[] args = new object[1];
                    args[0] = "error Test";
                    throw (Exception)Activator.CreateInstance(exceptionType, args);
                }
                return new FetchResponse();
            };
            return result;
        }

        private void CreateSuccessfulSendMock(BrokerRouterProxy routerProxy)
        {
            routerProxy.Connection1.FetchResponseFunction = ShouldReturnValidMessage;
            routerProxy.Connection1.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
            routerProxy.Connection2.FetchResponseFunction = ShouldReturnValidMessage;
            routerProxy.Connection2.MetadataResponseFunction = BrokerRouterProxy.CreateMetadataResponseWithMultipleBrokers;
        }

        private Task<FetchResponse> ShouldReturnValidMessage()
        {
            return Task.FromResult(new FetchResponse());
        }
    }
}