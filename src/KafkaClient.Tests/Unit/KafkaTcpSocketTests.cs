using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Connections;
using NUnit.Framework;

#pragma warning disable 1998

namespace KafkaClient.Tests.Unit
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    public class KafkaTcpSocketTests
    {
        [Test]
        public void KafkaTcpSocketShouldConstruct()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                Assert.That(test, Is.Not.Null);
                Assert.That(test.Endpoint, Is.EqualTo(endpoint));
            }
        }

        #region Connection Tests...

        [Test]
        public async Task ConnectionShouldStartDedicatedThreadOnConstruction()
        {
            var count = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => Interlocked.Increment(ref count));
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            {
                await TaskTest.WaitFor(() => count > 0);
                Assert.That(count, Is.GreaterThan(0));
            }
        }

        [Test]
        public async Task ConnectionShouldAttemptMultipleTimesWhenConnectionFails()
        {
            var count = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => Interlocked.Increment(ref count));
            using (var test = new TcpSocket(Endpoint.Resolve(new Uri("http://localhost:1"), TestConfig.Log), config, TestConfig.Log))
            {
                var write = test.WriteAsync(new DataPayload(1.ToBytes()), CancellationToken.None); //will force a connection
                await TaskTest.WaitFor(() => count > 1, 10000);
                Assert.That(count, Is.GreaterThan(1));
            }
        }

        #endregion Connection Tests...

        #region Dispose Tests...

        [Test]
        public async Task KafkaTcpSocketShouldDisposeEvenWhilePollingToReconnect()
        {
            int connectionAttempt = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, a, _) => {
                                                         connectionAttempt = a;
                                                     });
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            {
                var taskResult = test.ReadAsync(4, CancellationToken.None);

                await TaskTest.WaitFor(() => connectionAttempt > 1);

                test.Dispose();
                await Task.WhenAny(taskResult, Task.Delay(1000)).ConfigureAwait(false);

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
            }
        }

        [Test]
        public async Task KafkaTcpSocketShouldDisposeEvenWhileAwaitingReadAndThrowException()
        {
            int readSize = 0;
            var config = new ConnectionConfiguration(onReading: (e, size) => readSize = size);
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            {
                var taskResult = test.ReadAsync(4, CancellationToken.None);

                await TaskTest.WaitFor(() => readSize > 0);

                using (test) { }

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>());
            }
        }

        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhileWriting()
        {
            int writeSize = 0;
            var config = new ConnectionConfiguration(onWriting: (e, payload) => writeSize = payload.Buffer.Length);
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            {
                var taskResult = test.WriteAsync(new DataPayload(4.ToBytes()), CancellationToken.None);

                var wait = TaskTest.WaitFor(() => writeSize > 0);

                using (test)
                {
                } //allow the sockets to set

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(20));

                Assert.That(taskResult.IsCompleted, Is.True);
                Assert.That(taskResult.IsFaulted, Is.True, "Task should result indicate a fault.");
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ObjectDisposedException>(),
                    "Exception should be a disposed exception.");
            }
        }

        #endregion Dispose Tests...

        #region Read Tests...

        [Test]
        public void ReadShouldCancelWhileAwaitingResponse()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                var count = 0;
                var semaphore = new SemaphoreSlim(0);
                var token = new CancellationTokenSource();

                test.ReadAsync(4, token.Token).ContinueWith(t =>
                    {
                        Interlocked.Increment(ref count);
                        Assert.That(t.IsCanceled, Is.True, "Task should be set to cancelled when disposed.");
                        semaphore.Release();
                    });

                Thread.Sleep(100);
                token.Cancel();

                semaphore.Wait(TimeSpan.FromSeconds(1));
                Assert.That(count, Is.EqualTo(1), "Read should have cancelled and incremented count.");
            }
        }

        [Test]
        public async Task ReadShouldCancelWhileAwaitingReconnection()
        {
            int connectionAttempt = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, attempt, elapsed) => connectionAttempt = attempt);
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            using (var token = new CancellationTokenSource())
            {
                var taskResult = test.ReadAsync(4, token.Token);

                await TaskTest.WaitFor(() => connectionAttempt > 1);

                token.Cancel();

                taskResult.SafeWait(TimeSpan.FromMilliseconds(1000));

                Assert.That(taskResult.IsCanceled, Is.True);
            }
        }

        [Test]
        public async Task SocketShouldReconnectEvenAfterCancelledRead()
        {
            int connectionAttempt = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, attempt, elapsed) => Interlocked.Exchange(ref connectionAttempt, attempt));
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            using (var token = new CancellationTokenSource())
            {
                var taskResult = test.ReadAsync(4, token.Token);

                await TaskTest.WaitFor(() => connectionAttempt > 1);

                var attemptsMadeSoFar = connectionAttempt;

                token.Cancel();

                await TaskTest.WaitFor(() => connectionAttempt > attemptsMadeSoFar);

                Assert.That(connectionAttempt, Is.GreaterThan(attemptsMadeSoFar));
            }
        }

        [Test]
        public async Task ReadShouldBlockUntilAllBytesRequestedAreReceived()
        {
            var sendCompleted = 0;
            var bytesReceived = 0;
            var config = new ConnectionConfiguration(onReadChunk: (e, size, remaining, read, elapsed) => Interlocked.Add(ref bytesReceived, read));
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            {
                var resultTask = test.ReadAsync(4, CancellationToken.None).ContinueWith(t =>
                    {
                        Interlocked.Increment(ref sendCompleted);
                        return t.Result;
                    });

                Console.WriteLine("Sending first 3 bytes...");
                server.HasClientConnected.Wait(TimeSpan.FromMilliseconds(1000));
                var sendInitialBytes = server.SendDataAsync(new byte[] { 0, 0, 0 }).Wait(TimeSpan.FromSeconds(10));
                Assert.That(sendInitialBytes, Is.True, "First 3 bytes should have been sent.");

                Console.WriteLine("Ensuring task blocks...");
                await TaskTest.WaitFor(() => bytesReceived > 0);
                Assert.That(resultTask.IsCompleted, Is.False, "Task should still be running, blocking.");
                Assert.That(sendCompleted, Is.EqualTo(0), "Should still block even though bytes have been received.");
                Assert.That(bytesReceived, Is.EqualTo(3), "Three bytes should have been received and we are waiting on the last byte.");

                Console.WriteLine("Sending last byte...");
                var sendLastByte = server.SendDataAsync(new byte[] { 0 }).Wait(TimeSpan.FromSeconds(10));
                Assert.That(sendLastByte, Is.True, "Last byte should have sent.");

                Console.WriteLine("Ensuring task unblocks...");
                await TaskTest.WaitFor(() => resultTask.IsCompleted);
                Assert.That(bytesReceived, Is.EqualTo(4), "Should have received 4 bytes.");
                Assert.That(resultTask.IsCompleted, Is.True, "Task should have completed.");
                Assert.That(sendCompleted, Is.EqualTo(1), "Task ContinueWith should have executed.");
                Assert.That(resultTask.Result.Length, Is.EqualTo(4), "Result of task should be 4 bytes.");
            }
        }

        [Test]
        public void ReadShouldBeAbleToReceiveMoreThanOnce()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                const int firstMessage = 99;
                const string secondMessage = "testmessage";

                Console.WriteLine("Sending first message to receive...");
                var send = server.SendDataAsync(firstMessage.ToBytes());

                var firstResponse = test.ReadAsync(4, CancellationToken.None).Result.ToInt32();
                Assert.That(firstResponse, Is.EqualTo(firstMessage));

                Console.WriteLine("Sending second message to receive...");
                server.SendDataAsync(secondMessage);

                var secondResponse = Encoding.ASCII.GetString(test.ReadAsync(secondMessage.Length, CancellationToken.None).Result);
                Assert.That(secondResponse, Is.EqualTo(secondMessage));
            }
        }

        [Test]
        public void ReadShouldBeAbleToReceiveMoreThanOnceAsyncronously()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                const int firstMessage = 99;
                const int secondMessage = 100;

                Console.WriteLine("Sending first message to receive...");
                var send1 = server.SendDataAsync(firstMessage.ToBytes());
                var firstResponseTask = test.ReadAsync(4, CancellationToken.None);

                Console.WriteLine("Sending second message to receive...");
                var send2 = server.SendDataAsync(secondMessage.ToBytes());
                var secondResponseTask = test.ReadAsync(4, CancellationToken.None);

                Assert.That(firstResponseTask.Result.ToInt32(), Is.EqualTo(firstMessage));
                Assert.That(secondResponseTask.Result.ToInt32(), Is.EqualTo(secondMessage));
            }
        }

        [Test]
        public void ReadShouldNotLoseDataFromStreamOverMultipleReads()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                const int firstMessage = 99;
                const string secondMessage = "testmessage";
                var bytes = Encoding.UTF8.GetBytes(secondMessage);

                var payload = new KafkaWriter()
                    .Write(firstMessage)
                    .Write(bytes, false);

                //send the combined payload
                var send = server.SendDataAsync(payload.ToBytesNoLength());

                var firstResponse = test.ReadAsync(4, CancellationToken.None).Result.ToInt32();
                Assert.That(firstResponse, Is.EqualTo(firstMessage));

                var secondResponse = Encoding.ASCII.GetString(test.ReadAsync(secondMessage.Length, CancellationToken.None).Result);
                Assert.That(secondResponse, Is.EqualTo(secondMessage));
            }
        }

        [Test]
        public async Task ReadShouldThrowServerDisconnectedExceptionWhenDisconnected()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var socket = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                var resultTask = socket.ReadAsync(4, CancellationToken.None);

                //wait till connected
                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);

                server.DropConnection();

                await TaskTest.WaitFor(() => server.DisconnectionEventCount > 0);

                resultTask.ContinueWith(t => resultTask = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(resultTask.IsFaulted, Is.True);
                Assert.That(resultTask.Exception.InnerException, Is.TypeOf<ConnectionException>());
            }
        }

        [Test]
        public async Task WhenNoConnectionThrowSocketException()
        {
            var socket = new TcpSocket(new Endpoint(new Uri("http://not.com"), null), log: TestConfig.Log);

            var resultTask = socket.ReadAsync(4, CancellationToken.None);
            Assert.ThrowsAsync<ConnectionException>(async () => await resultTask);
        }

        [Test]
        public async Task WhenNoConnectionThrowSocketExceptionAfterMaxRetry()
        {
            var reconnectionAttempt = 0;
            var config = new ConnectionConfiguration(onConnecting: (endpoint, attempt, elapsed) => Interlocked.Increment(ref reconnectionAttempt));
            var socket = new TcpSocket(new Endpoint(new Uri("http://not.com"), null), config, TestConfig.Log);
            var resultTask = socket.ReadAsync(4, CancellationToken.None);
            Assert.ThrowsAsync<ConnectionException>(async () => await resultTask);
            Assert.That(reconnectionAttempt, Is.AtLeast(ConnectionConfiguration.Defaults.MaxConnectionAttempts + 1));
            Assert.That(reconnectionAttempt, Is.AtMost(ConnectionConfiguration.Defaults.MaxConnectionAttempts + 2));
        }

        [Test]
        public async Task ShouldReconnectAfterLosingConnectionAndBeAbleToStartNewRead()
        {
            var log = TestConfig.Log;
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            {
                var disconnects = 0;
                var connects = 0;
                TaskCompletionSource<int> disconnectEvent = new TaskCompletionSource<int>();

                server.OnClientConnected += () => Interlocked.Increment(ref connects);
                server.OnClientDisconnected += () => Interlocked.Increment(ref disconnects);
                var config = new ConnectionConfiguration(onDisconnected: (e, exception) => {
                    log.Info(() => LogEvent.Create("inside onDisconnected"));
                                                             disconnectEvent.TrySetResult(1);
                                                         });
                using (var socket = new TcpSocket(endpoint, config, log)) {

                    //wait till connected
                    await TaskTest.WaitFor(() => connects > 0);
                    log.Info(() => LogEvent.Create("connects > 0"));
                    var readTask = socket.ReadAsync(4, CancellationToken.None);

                    log.Info(() => LogEvent.Create("Connected: Dropping connection..."));
                    server.DropConnection();

                    //wait till Disconnected
                    log.Info(() => LogEvent.Create("Waiting for onDisconnected"));
                    await Task.WhenAny(disconnectEvent.Task, Task.Delay(100000));
                    Assert.IsTrue(disconnectEvent.Task.IsCompleted, "Server should have disconnected the client.");
                    Assert.ThrowsAsync<ConnectionException>(async () => await readTask);
                    await TaskTest.WaitFor(() => connects == 2, 6000);
                    log.Info(() => LogEvent.Create("connects == 2"));
                    Assert.That(connects, Is.EqualTo(2), "Socket should have reconnected.");

                    var readTask2 = socket.ReadAsync(4, CancellationToken.None);

                    log.Info(() => LogEvent.Create("sending data (99)"));
                    await server.SendDataAsync(99.ToBytes());
                    var result = await readTask2;
                    Assert.That(result.ToInt32(), Is.EqualTo(99), "Socket should have received the 4 bytes.");
                }
            }
        }

        [Test]
        public void ReadShouldStackReadRequestsAndReturnOneAtATime()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            {
                var messages = new[] { "test1", "test2", "test3", "test4" };
                var expectedLength = "test1".Length;

                var payload = new KafkaWriter().Write(messages);

                using (var socket = new TcpSocket(endpoint, log: TestConfig.Log)) {
                    var tasks = messages.Select(x => socket.ReadAsync(x.Length, CancellationToken.None)).ToArray();

                    var send = server.SendDataAsync(payload.ToBytes());

                    Task.WaitAll(tasks);

                    foreach (var task in tasks)
                    {
                        Assert.That(task.Result.Length, Is.EqualTo(expectedLength));
                    }
                }
            }
        }

        #endregion Read Tests...

        #region Write Tests...

        [Test]
        public async Task WriteAsyncShouldSendData()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                const int testData = 99;
                int result = 0;

                server.OnBytesReceived += data => result = data.ToInt32();

                test.WriteAsync(new DataPayload(testData.ToBytes()), CancellationToken.None).Wait(TimeSpan.FromSeconds(1));
                await TaskTest.WaitFor(() => result > 0);
                Assert.That(result, Is.EqualTo(testData));
            }
        }

        [Test]
        public async Task WriteAsyncShouldAllowMoreThanOneWrite()
        {
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                const int testData = 99;
                var results = new List<byte>();

                server.OnBytesReceived += results.AddRange;

                Task.WaitAll(test.WriteAsync(new DataPayload(testData.ToBytes()), CancellationToken.None), test.WriteAsync(new DataPayload(testData.ToBytes()), CancellationToken.None));
                await TaskTest.WaitFor(() => results.Count >= 8);
                Assert.That(results.Count, Is.EqualTo(8));
            }
        }

        [Test]
        public void WriteAndReadShouldBeAsynchronous()
        {
            //     for (int j = 0; j < 1000; j++)
            //     {
            var expected = new List<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
            var write = new ConcurrentBag<int>();
            var read = new ConcurrentBag<int>();

            AutoResetEvent allEventAreArrived = new AutoResetEvent(false);
            AutoResetEvent allrReadEventAreArrived = new AutoResetEvent(false);
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                server.OnBytesReceived += data =>
                {
                    var d = data.Batch(4).Select(x => x.ToArray().ToInt32());
                    foreach (var item in d)
                    {
                        write.Add(item);
                    }

                    if (expected.Count == write.Count)
                    {
                        allEventAreArrived.Set();
                    }
                };
                var tasks = Enumerable.Range(1, 10)
                    .SelectMany(i => new[]
                        {
                            test.WriteAsync(new DataPayload(i.ToBytes()), CancellationToken.None),
                            test.ReadAsync(4, CancellationToken.None).ContinueWith(t =>
                            {
                                read.Add(t.Result.ToInt32());
                                if (read.Count == expected.Count)
                                {
                                    allrReadEventAreArrived.Set();
                                }
                            }),
                            server.SendDataAsync(i.ToBytes())
                        }).ToArray();

                Task.WaitAll(tasks);
                Assert.IsTrue(allEventAreArrived.WaitOne(3000), "not Get all write event in time");
                Assert.IsTrue(allrReadEventAreArrived.WaitOne(3000), "not Get all  read event in time");
                var w = write.OrderBy(x => x);
                var r = read.OrderBy(x => x);

                for (int i = 0; i < expected.Count; i++)
                {
                    //  _log.InfoFormat("write was {0}  expected {1}", w.ElementAt(i), expected[i]);
                    Assert.That(w.ElementAt(i), Is.EqualTo(expected[i]));
                }
                for (int i = 0; i < expected.Count; i++)
                {
                    //     _log.InfoFormat("read was {0}  expected {1}", r.ElementAt(i), expected[i]);
                    Assert.That(r.ElementAt(i), Is.EqualTo(expected[i]));
                }
            }
            //   }
        }

        [Test]
        public void WriteShouldHandleLargeVolumeSendAsynchronously()
        {
            AutoResetEvent allEventAreArrived = new AutoResetEvent(false);
            var write = new ConcurrentBag<int>();
            int percent = 0;
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, log: TestConfig.Log))
            {
                int numberOfWrite = 10000;
                server.OnBytesReceived += data =>
                {
                    var d = data.Batch(4).Select(x => x.ToArray().ToInt32());
                    foreach (var item in d)
                    {
                        write.Add(item);
                    }

                    if (write.Count % (numberOfWrite / 100) == 0)
                    {
                        Console.WriteLine("*************** done  percent:" + percent);
                        percent++;
                    }
                    if (write.Count == numberOfWrite)
                    {
                        allEventAreArrived.Set();
                    }
                };
                var tasks = Enumerable.Range(1, 10000).SelectMany(i => new[] { test.WriteAsync(new DataPayload(i.ToBytes()), CancellationToken.None)}).ToArray();

                Task.WaitAll(tasks);
                Assert.IsTrue(allEventAreArrived.WaitOne(2000), "not get all event on time");
                Assert.That(write.OrderBy(x => x), Is.EqualTo(Enumerable.Range(1, numberOfWrite)));
            }
        }

        [Test]
        public async Task WriteShouldCancelWhileSendingData()
        {
            var writeAttempts = 0;
            var config = new ConnectionConfiguration(onWriting: (e, payload) => Interlocked.Increment(ref writeAttempts));
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var server = new FakeTcpServer(TestConfig.Log, endpoint.IP.Port))
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            using (var token = new CancellationTokenSource())
            {
                var write = test.WriteAsync(new DataPayload(1.ToBytes()), token.Token);

                await TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                await TaskTest.WaitFor(() => writeAttempts > 0);

                Assert.That(writeAttempts, Is.EqualTo(1), "Socket should have attempted to write.");

                //create a buffer write that will take a long time
                var data = Enumerable.Range(0, 100000000).Select(b => (byte)b).ToArray();
                token.Cancel();
                var taskResult = test.WriteAsync(
                        new DataPayload(data),
                        token.Token);
                await Task.WhenAny(taskResult, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);

                Assert.That(taskResult.IsCanceled, Is.True, "Task should have cancelled.");
            }
        }

        [Test]
        public void WriteShouldCancelWhileAwaitingReconnection()
        {
            int connectionAttempt = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, attempt, elapsed) => connectionAttempt = attempt);
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            using (var token = new CancellationTokenSource())
            {
                var taskResult = test.WriteAsync(new DataPayload(1.ToBytes()), token.Token);

                var wait = TaskTest.WaitFor(() => connectionAttempt > 1);

                token.Cancel();

                taskResult.SafeWait(TimeSpan.FromMilliseconds(1000));

                Assert.That(taskResult.IsCanceled, Is.True);
            }
        }

        [Test]
        public async Task SocketShouldReconnectEvenAfterCancelledWrite()
        {
            int connectionAttempt = 0;
            var config = new ConnectionConfiguration(onConnecting: (e, attempt, elapsed) => Interlocked.Exchange(ref connectionAttempt, attempt));
            var endpoint = Endpoint.Resolve(TestConfig.ServerUri(), TestConfig.Log);
            using (var test = new TcpSocket(endpoint, config, TestConfig.Log))
            using (var token = new CancellationTokenSource())
            {
                var taskResult = test.WriteAsync(new DataPayload(1.ToBytes()), token.Token);

                var wait = TaskTest.WaitFor(() => connectionAttempt > 1);

                var attemptsMadeSoFar = connectionAttempt;

                token.Cancel();

                await TaskTest.WaitFor(() => connectionAttempt > attemptsMadeSoFar);

                Assert.That(connectionAttempt, Is.GreaterThan(attemptsMadeSoFar));
            }
        }

        #endregion Write Tests...
    }
}