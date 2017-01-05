using System.Collections.Generic;
using System.Linq;
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
    [Category("Unit")]
    [TestFixture]
    public class KafkaMetadataGetAsyncTests
    {
        private MemoryLog _log;
        private IRouter _brokerRouter;

        [SetUp]
        public void Setup()
        {
            _log = new MemoryLog();
            //_log = Substitute.ForPartsOf<MemoryLog>();
            _brokerRouter = Substitute.For<IRouter>();
            _brokerRouter.Log.ReturnsForAnyArgs(_log);
            _brokerRouter.Configuration.ReturnsForAnyArgs(new RouterConfiguration());
        }

        [Test]
        [TestCase(ErrorResponseCode.LeaderNotAvailable)]
        [TestCase(ErrorResponseCode.GroupLoadInProgress)]
        [TestCase(ErrorResponseCode.GroupCoordinatorNotAvailable)]
        public async Task ShouldRetryWhenReceiveAnRetryErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(errorCode), x => CreateMetadataResponse(errorCode));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var response = await _brokerRouter.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 1: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
            });

            Assert.That(_log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
            Assert.That(_log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 1: Will retry in")));
            Assert.That(_log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt")), Is.EqualTo(2));
        }

        [Test]
        public async Task ShouldRetryWhenReceiveBrokerIdNegativeOne()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>(), Arg.Any<IRequestContext>())
                .Returns(x => CreateMetadataResponse(-1, "123", 1), x => CreateMetadataResponse(ErrorResponseCode.None));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var response = await _brokerRouter.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None);

            Received.InOrder(() =>
            {
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
                //_log.OnLogged.Invoke(LogLevel.Warn, It.Is<LogEvent>(e => e.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
                conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
            });

            Assert.That(_log.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt 0: Will retry in")));
            Assert.That(_log.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Failed metadata request on attempt")), Is.EqualTo(1));
        }

        [Test]
        public void ShouldReturnWhenNoErrorReceived()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.None));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var source = new CancellationTokenSource();
            var response = _brokerRouter.GetMetadataAsync(new MetadataRequest("Test"), source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Test]
        public void ShouldReturnWhenNoErrorReceivedAndTopicsNotSpecified()
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>())
                .Returns(x => CreateMetadataResponse(ErrorResponseCode.None));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            var source = new CancellationTokenSource();
            var response = _brokerRouter.GetMetadataAsync(new MetadataRequest(), source.Token);
            source.Cancel();

            conn.Received(1).SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>());
        }

        [Test]
        [TestCase(ErrorResponseCode.Unknown)]
        [TestCase(ErrorResponseCode.InvalidTopic)]
        [TestCase(ErrorResponseCode.InvalidRequiredAcks)]
        public async Task ShouldThrowExceptionWhenNotARetriableErrorCode(ErrorResponseCode errorCode)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(errorCode));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            Assert.ThrowsAsync<RequestException>(() => _brokerRouter.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        public void ShouldThrowExceptionWhenHostIsMissing(string host)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(1, host, 1));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            Assert.ThrowsAsync<ConnectionException>(() => _brokerRouter.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

        [Test]
        [TestCase(0)]
        [TestCase(-1)]
        public void ShouldThrowExceptionWhenPortIsMissing(int port)
        {
            var conn = Substitute.For<IConnection>();

            conn.SendAsync(Arg.Any<IRequest<MetadataResponse>>(), Arg.Any<CancellationToken>()).Returns(x => CreateMetadataResponse(1, "123", port));

            _brokerRouter.Connections.ReturnsForAnyArgs(new List<IConnection> {conn});
            Assert.ThrowsAsync<ConnectionException>(() => _brokerRouter.GetMetadataAsync(new MetadataRequest("Test"), CancellationToken.None));
        }

#pragma warning disable 1998
        private Task<MetadataResponse> CreateMetadataResponse(int brokerId, string host, int port)
        {
            var tcs = new TaskCompletionSource<MetadataResponse>();
            tcs.SetResult(new MetadataResponse(new [] { new KafkaClient.Protocol.Broker(brokerId, host, port) }, new MetadataResponse.Topic[] {}));
            return tcs.Task;
        }

        private async Task<MetadataResponse> CreateMetadataResponse(ErrorResponseCode errorCode)
        {
            return new MetadataResponse(new KafkaClient.Protocol.Broker[] {}, new [] { new MetadataResponse.Topic("Test", errorCode, new MetadataResponse.Partition[] {})});
        }
#pragma warning restore 1998
    }
}