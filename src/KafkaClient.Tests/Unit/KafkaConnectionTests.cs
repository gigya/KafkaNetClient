using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using KafkaClient.Protocol;
using Nito.AsyncEx;
using NUnit.Framework;
using NUnit.Framework.Internal;

namespace KafkaClient.Tests.Unit
{
    [Category("Unit")]
    [TestFixture]
    public class KafkaConnectionTests
    {
        #region Construct...

        [Test]
        public async Task ShouldStartReadPollingOnConstruction()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            using (var conn = new Connection(socket, log: TestConfig.Log))
            {
                await TaskTest.WaitFor(() => conn.IsReaderAlive);
                Assert.That(conn.IsReaderAlive, Is.True);
            }
        }

        [Test]
        public void ShouldReportServerUriOnConstruction()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            using (var conn = new Connection(socket, log: TestConfig.Log))
            {
                Assert.That(conn.Endpoint, Is.EqualTo(endpoint));
            }
        }

        #endregion Construct...

        #region Dispose Tests...

        [Test]
        public async Task ShouldDisposeWithoutExceptionThrown()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                var conn = new Connection(socket, log: TestConfig.Log);
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                using (conn) { }
            }
        }

        [Test]
        public void ShouldDisposeWithoutExceptionEvenWhileCallingSendAsync()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            using (var conn = new Connection(socket, log: TestConfig.Log))
            {
                var task = conn.SendAsync(new MetadataRequest(), CancellationToken.None);
                task.Wait(TimeSpan.FromMilliseconds(1000));
                Assert.That(task.IsCompleted, Is.False, "The send task should still be pending.");
            }
        }

        #endregion Dispose Tests...

        #region Read Tests...

        [Test]
        public async Task KafkaConnectionShouldLogDisconnectAndRecover([Values(3, 4)] int connectionAttempts)
        {
            var mockLog = new MemoryLog();
            var disconnected = 0;
            var connected = 0;

            var config = new ConnectionConfiguration(
                onDisconnected: (e, exception) => {
                    Interlocked.Increment(ref disconnected);
                },
                onConnected: (e, attempt, elapsed) => {
                    Interlocked.Increment(ref connected);
                });

            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var socket = new TcpSocket(endpoint, config, TestConfig.Log))
            using (new Connection(socket, config, log: mockLog))
            {
                for (var connectionAttempt = 1; connectionAttempt <= connectionAttempts; connectionAttempt++)
                {
                    var currentAttempt = connectionAttempt;
                    await TaskTest.WaitFor(() => server.ConnectionEventcount == currentAttempt);
                    Assert.That(server.ConnectionEventcount, Is.EqualTo(connectionAttempt));
                    await server.SendDataAsync(CreateCorrelationMessage(connectionAttempt));
                    TestConfig.Log.Write(LogLevel.Info, () => LogEvent.Create($"Sent CONNECTION attempt {connectionAttempt}"));
                    await TaskTest.WaitFor(() => connected == disconnected, 200);

                    Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Info && e.Item2.Message.StartsWith("Polling receive thread has recovered on ")), Is.EqualTo(currentAttempt-1));

                    TestConfig.Log.Write(LogLevel.Info, () => LogEvent.Create($"Dropping CONNECTION attempt {connectionAttempt}"));
                    server.DropConnection();
                    await TaskTest.WaitFor(() => disconnected == currentAttempt);

                    Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Error && e.Item2.Message.StartsWith("Polling failure on")), Is.AtLeast(currentAttempt));
                }
            }
        }

        [Test]
        public async Task KafkaConnectionShouldSkipPartiallyReadMessage()
        {
            var mockLog = new MemoryLog();
            var bytesRead = 0;

            var config = new ConnectionConfiguration(onReadChunk: (e, size, remaining, read, elapsed) => Interlocked.Add(ref bytesRead, read));

            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var socket = new TcpSocket(endpoint, config, TestConfig.Log))
            using (new Connection(socket, config, log: mockLog))
            {
                // send size
                var size = 200;
                await server.SendDataAsync(size.ToBytes());
                var randomizer = Randomizer.CreateRandomizer();
                var firstBytes = new byte[99];
                for (var i = 0; i < firstBytes.Length; i++)
                {
                    firstBytes[i] = randomizer.NextByte();
                }

                // send half of payload
                await server.SendDataAsync(firstBytes);
                await TaskTest.WaitFor(() => bytesRead == firstBytes.Length);

                //Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith("Skipping")), Is.EqualTo(0));

                server.DropConnection();

                // send half of payload should be skipped
                var lastBytes = new byte[size];
                for (var i = 0; i < lastBytes.Length; i++)
                {
                    lastBytes[i] = randomizer.NextByte();
                }
                await server.SendDataAsync(lastBytes);
                await TaskTest.WaitFor(() => bytesRead >= size);

                Assert.That(mockLog.LogEvents.Count(e => e.Item1 == LogLevel.Warn && e.Item2.Message.StartsWith($"Skipping {size - firstBytes.Length} bytes on")), Is.EqualTo(1));
            }
        }

        [Test]
        public async Task ReadShouldIgnoreMessageWithUnknownCorrelationId()
        {
            const int correlationId = 99;
            var receivedData = false;

            var mockLog = new MemoryLog();

            var config = new ConnectionConfiguration(onRead: (e, buffer, elapsed) => receivedData = true);
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var socket = new TcpSocket(endpoint, config, mockLog))
            using (var conn = new Connection(socket, config, log: mockLog))
            {
                //send correlation message
                server.SendDataAsync(CreateCorrelationMessage(correlationId)).Wait(TimeSpan.FromSeconds(5));

                //wait for connection
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                await TaskTest.WaitFor(() => receivedData);

                // shortly after receivedData, but still after
                await TaskTest.WaitFor(() => mockLog.LogEvents.Any(e => e.Item1 == LogLevel.Warn && e.Item2.Message == $"Unexpected response from {endpoint} with correlation id {correlationId} (not in request queue)."));
            }
        }

        #endregion Read Tests...

        #region Send Tests...

        [Test]
        public async Task SendAsyncShouldTimeoutWhenSendAsyncTakesTooLong()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            using (var conn = new Connection(socket, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(1)), log: TestConfig.Log))
            {
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var taskResult = conn.SendAsync(new MetadataRequest(), CancellationToken.None);

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromMilliseconds(100));

                Assert.That(taskResult.IsFaulted, Is.True, "Task should have reported an exception.");
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<TimeoutException>());
            }
        }

        [Test, Timeout(15000)]
        public async Task SendAsyncShouldNotAllowResponseToTimeoutWhileAwaitingKafkaToEstableConnection()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            using (var conn = new Connection(socket, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(1000)), log: TestConfig.Log))
            {
                Console.WriteLine("SendAsync blocked by reconnection attempts...");
                var taskResult = conn.SendAsync(new MetadataRequest(), CancellationToken.None);

                Console.WriteLine("Task result should be WaitingForActivation...");
                Assert.That(taskResult.IsFaulted, Is.False);
                Assert.That(taskResult.Status, Is.EqualTo(TaskStatus.WaitingForActivation));

                Console.WriteLine("Starting server to establish connection...");
                using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
                {
                    server.OnClientConnected += () => Console.WriteLine("Client connected...");
                    server.OnBytesReceived += b =>
                    {
                        var request = KafkaDecoder.DecodeHeader(b);
                        AsyncContext.Run(async () => await server.SendDataAsync(MessageHelper.CreateMetadataResponse(request.CorrelationId, "Test")));
                    };

                    await Task.WhenAny(taskResult, Task.Delay(TimeSpan.FromSeconds(10)));

                    Assert.That(taskResult.IsFaulted, Is.False);
                    Assert.That(taskResult.IsCanceled, Is.False);
                    await taskResult;
                    Assert.That(taskResult.Status, Is.EqualTo(TaskStatus.RanToCompletion));
                }
            }
        }

        [Test]
        public async Task SendAsyncShouldUseStatictVersionInfo()
        {
            IRequestContext context = null;
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            using (var conn = new Connection(socket, new ConnectionConfiguration(requestTimeout: TimeSpan.FromSeconds(1000), versionSupport: VersionSupport.Kafka10), log: TestConfig.Log))
            {
                server.OnBytesReceived += data =>
                {
                    context = KafkaDecoder.DecodeHeader(data);
                    var send = server.SendDataAsync(KafkaDecoder.EncodeResponseBytes(context, new FetchResponse()));
                };

                await conn.SendAsync(new FetchRequest(new FetchRequest.Topic("Foo", 0, 0)), CancellationToken.None);
                await TaskTest.WaitFor(() => context != null);

                Assert.That(context.ApiVersion.Value, Is.EqualTo(2));
            }
        }

        [Test]
        public async Task SendAsyncShouldTimeoutMultipleMessagesAtATime()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            using (var conn = new Connection(socket, new ConnectionConfiguration(requestTimeout: TimeSpan.FromMilliseconds(100)), log: TestConfig.Log))
            {
                server.HasClientConnected.Wait(TimeSpan.FromSeconds(3));
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var tasks = new[] {
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None),
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None),
                    conn.SendAsync(new MetadataRequest(), CancellationToken.None)
                };

                await TaskTest.WaitFor(() => tasks.All(t => t.IsFaulted));
                foreach (var task in tasks)
                {
                    Assert.That(task.IsFaulted, Is.True, "Task should have faulted.");
                    Assert.That(task.Exception.InnerException, Is.TypeOf<TimeoutException>(), "Task fault has wrong type.");
                }
            }
        }

        #endregion Send Tests...

        private static byte[] CreateCorrelationMessage(int id)
        {
            return new KafkaWriter().Write(id).ToBytes();
        }
    }
}