using System;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using Nito.AsyncEx;

namespace KafkaClient.Connections
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled internally.
    /// </summary>
    public class TcpSocket : ITcpSocket
    {
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly CancellationTokenRegistration _disposeRegistration;
        private int _disposeCount;

        private readonly ILog _log;
        private readonly IConnectionConfiguration _configuration;

        private readonly AsyncProducerConsumerQueue<SocketPayloadWriteTask> _writeTaskQueue;
        private readonly AsyncProducerConsumerQueue<SocketPayloadReadTask> _readTaskQueue;
        // ReSharper disable NotAccessedField.Local
        private readonly Task _socketTask;
        // ReSharper restore NotAccessedField.Local
        private readonly AsyncLock _clientLock = new AsyncLock();
        private TcpClient _client;

        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="configuration">Configuration for timeouts and retries.</param>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="endpoint">The IP endpoint to connect to.</param>
        public TcpSocket(Endpoint endpoint, IConnectionConfiguration configuration = null, ILog log = null)
        {
            Endpoint = endpoint;
            _log = log ?? TraceLog.Log;
            _configuration = configuration ?? new ConnectionConfiguration();
            _writeTaskQueue = new AsyncProducerConsumerQueue<SocketPayloadWriteTask>();
            _readTaskQueue = new AsyncProducerConsumerQueue<SocketPayloadReadTask>();

            _disposeRegistration = _disposeToken.Token.Register(() => {
                _writeTaskQueue.CompleteAdding();
                _readTaskQueue.CompleteAdding();
            });

            // dedicate a long running task to the read/write operations
            _socketTask = Task.Factory.StartNew(DedicatedSocketTask, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        #region Interface Implementation...

        /// <inheritdoc />
        public Endpoint Endpoint { get; }

        /// <inheritdoc />
        public async Task<byte[]> ReadAsync(int readSize, CancellationToken cancellationToken)
        {
            var readTask = new SocketPayloadReadTask(readSize, cancellationToken);
            await _readTaskQueue.EnqueueAsync(readTask, cancellationToken).ConfigureAwait(false);
            return await readTask.Tcs.Task.ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<DataPayload> WriteAsync(DataPayload payload, CancellationToken cancellationToken)
        {
            var writeTask = new SocketPayloadWriteTask(payload, cancellationToken);
            await _writeTaskQueue.EnqueueAsync(writeTask, cancellationToken).ConfigureAwait(false);
            _configuration.OnWriteEnqueued?.Invoke(Endpoint, payload);
            return await writeTask.Tcs.Task.ConfigureAwait(false);
        }

        #endregion Interface Implementation...

        /// <summary>
        /// Stop all pendding task when can not establish connection in max retry,
        /// but keep trying to recove and connect to this connection.
        /// Only the broker router can dispose of it.
        /// </summary>
        /// <returns></returns>
        private async Task DedicatedSocketTask()
        {
            while (!_disposeToken.IsCancellationRequested) {
                // block here until we can get connections then start loop pushing data through network stream
                try {
                    var netStreamTask = GetStreamAsync();
                    if (await netStreamTask.IsCancelled(_disposeToken.Token).ConfigureAwait(false)) {
                        var disposedException = new ObjectDisposedException($"Object is disposing (TcpSocket for {Endpoint})");
                        await SetExceptionToAllPendingTasksAsync(disposedException).ConfigureAwait(false);
                        _configuration.OnDisconnected?.Invoke(Endpoint, disposedException);
                        return;
                    }

                    var netStream = await netStreamTask.ConfigureAwait(false);
                    await ProcessNetworkstreamTasks(netStream).ConfigureAwait(false);
                } catch (Exception ex) {
                    await SetExceptionToAllPendingTasksAsync(ex).ConfigureAwait(false);
                    _configuration.OnDisconnected?.Invoke(Endpoint, ex);
                }
            }
        }

        private async Task SetExceptionToAllPendingTasksAsync(Exception ex)
        {
            var wrappedException = WrappedException(ex);
            var failedWrites = await _writeTaskQueue.TryApplyAsync(p => p.Tcs.TrySetException(wrappedException), new CancellationToken(true)).ConfigureAwait(false);
            var failedReads = await _readTaskQueue.TryApplyAsync(p => p.Tcs.TrySetException(wrappedException), new CancellationToken(true)).ConfigureAwait(false);
            if (failedWrites > 0) {
                _log.Error(LogEvent.Create(ex, $"TcpSocket exception, cancelled {failedWrites} pending writes"));
            }
            if (failedReads > 0) {
                _log.Error(LogEvent.Create(ex, $"TcpSocket exception, cancelled {failedReads} pending reads"));
            }
        }

        private async Task ProcessNetworkstreamTasks(Stream netStream)
        {
            //reading/writing from network steam is not thread safe
            //Read and write operations can be performed simultaneously on an instance of the NetworkStream class without the need for synchronization.
            //As long as there is one unique thread for the write operations and one unique thread for the read operations, there will be no cross-interference
            //between read and write threads and no synchronization is required.
            //https://msdn.microsoft.com/en-us/library/z2xae4f4.aspx

            //Exception need to thrown immediately and not depend on the next task
            var readTask = ProcessNetworkstreamTask(netStream, _readTaskQueue, ProcessReadTaskAsync);
            var writeTask = ProcessNetworkstreamTask(netStream, _writeTaskQueue, ProcessWriteTasksAsync);
            await Task.WhenAny(readTask, writeTask).ConfigureAwait(false);
            if (_disposeToken.IsCancellationRequested) return;

            await ThrowTaskExceptionIfFaulted(readTask).ConfigureAwait(false);
            await ThrowTaskExceptionIfFaulted(writeTask).ConfigureAwait(false);
        }

        private static async Task ThrowTaskExceptionIfFaulted(Task task)
        {
            if (task.IsFaulted || task.IsCanceled) await task.ConfigureAwait(false);
        }

        private async Task ProcessNetworkstreamTask<T>(Stream stream, AsyncProducerConsumerQueue<T> queue, Func<Stream, T, Task> asyncProcess)
        {
            Task lastTask = Task.FromResult(true);
            try {
                while (!_disposeToken.IsCancellationRequested && stream != null) {
                    await lastTask.ConfigureAwait(false);
                    var takeResult = await queue.DequeueAsync(_disposeToken.Token).ConfigureAwait(false);
                    lastTask = asyncProcess(stream, takeResult);
                }
            } catch (InvalidOperationException) {
                // thrown from DequeueAsync if closed and emptry
            }
        }

        private async Task ProcessReadTaskAsync(Stream stream, SocketPayloadReadTask readTask)
        {
            using (readTask) {
                var totalBytesRead = 0;
                var timer = new Stopwatch();
                try {
                    _configuration.OnReading?.Invoke(Endpoint, readTask.ReadSize);
                    var buffer = new byte[readTask.ReadSize];
                    timer.Start();
                    while (totalBytesRead < readTask.ReadSize) {
                        var readSize = readTask.ReadSize - totalBytesRead;

                        _log.Debug(() => LogEvent.Create($"Reading ({readSize}? bytes) from {Endpoint}"));
                        _configuration.OnReadingChunk?.Invoke(Endpoint, readTask.ReadSize, totalBytesRead, timer.Elapsed);
                        var bytesRead = await stream.ReadAsync(buffer, totalBytesRead, readSize, readTask.CancellationToken).ConfigureAwait(false);
                        totalBytesRead += bytesRead;
                        var remainingBytes = readTask.ReadSize - totalBytesRead;
                        _configuration.OnReadChunk?.Invoke(Endpoint, readTask.ReadSize, remainingBytes, bytesRead, timer.Elapsed);
                        _log.Debug(() => LogEvent.Create($"Read {bytesRead} bytes from {Endpoint}{(readTask.CancellationToken.IsCancellationRequested ? " (cancelled)" : "")}"));

                        if (bytesRead <= 0) {
                            using (_client) {
                                _client = null;
                                if (_disposeToken.IsCancellationRequested) {
                                    _configuration.OnReadFailed?.Invoke(Endpoint, readTask.ReadSize, timer.Elapsed, new TaskCanceledException());
                                    return;
                                }

                                throw new ConnectionException(Endpoint);
                            }
                        }
                    }
                    timer.Stop();
                    _configuration.OnRead?.Invoke(Endpoint, buffer, timer.Elapsed);

                    readTask.Tcs.TrySetResult(buffer);
                    totalBytesRead = 0;
                } catch (Exception ex) {
                    timer.Stop();
                    _configuration.OnReadFailed?.Invoke(Endpoint, readTask.ReadSize, timer.Elapsed, ex);

                    Exception exception = null;
                    if (_disposeToken.IsCancellationRequested) {
                        exception = new ObjectDisposedException($"Object is disposing (TcpSocket for {Endpoint})", ex);
                    } else if ((_client == null || _client.Connected == false) && !(ex is ConnectionException)) {
                        exception = new ConnectionException(Endpoint, ex);
                    }

                    if (totalBytesRead > 0) {
                        readTask.Tcs.TrySetException(new PartialReadException(totalBytesRead, readTask.ReadSize, exception ?? ex));
                    } else {
                        readTask.Tcs.TrySetException(exception ?? ex);
                    }
                    if (_disposeToken.IsCancellationRequested) return;
                    if (exception != null) throw exception;
                    throw;
                }
            }
        }

        private async Task ProcessWriteTasksAsync(Stream stream, SocketPayloadWriteTask writeTask)
        {
            if (writeTask == null) return;

            using (writeTask) {
                var timer = new Stopwatch();
                try {
                    _log.Debug(() => LogEvent.Create($"Writing {writeTask.Payload.Buffer.Length} bytes with correlation id {writeTask.Payload.CorrelationId} to {Endpoint}"));
                    _configuration.OnWriting?.Invoke(Endpoint, writeTask.Payload);
                    timer.Start();
                    await stream.WriteAsync(writeTask.Payload.Buffer, 0, writeTask.Payload.Buffer.Length, _disposeToken.Token).ConfigureAwait(false);
                    timer.Stop();
                    _configuration.OnWritten?.Invoke(Endpoint, writeTask.Payload, timer.Elapsed);
                    _log.Debug(() => LogEvent.Create($"Wrote {writeTask.Payload.Buffer.Length} bytes with correlation id {writeTask.Payload.CorrelationId} to {Endpoint}"));
                    writeTask.Tcs.TrySetResult(writeTask.Payload);
                } catch (Exception ex) {
                    var wrappedException = WrappedException(ex);
                    _configuration.OnWriteFailed?.Invoke(Endpoint, writeTask.Payload, timer.Elapsed, wrappedException);
                    writeTask.Tcs.TrySetException(wrappedException);
                    throw;
                }
            }
        }

        private Exception WrappedException(Exception ex)
        {
            if (_disposeToken.IsCancellationRequested) {
                return new ObjectDisposedException($"Object is disposing (TcpSocket for {Endpoint})", ex);
            }
            return new ConnectionException($"Lost connection to {Endpoint}", ex) { Endpoint = Endpoint };
        }

        private async Task<NetworkStream> GetStreamAsync()
        {
            using (await _clientLock.LockAsync(_disposeToken.Token).ConfigureAwait(false)) {
                if ((_client == null || _client.Connected == false) && !_disposeToken.IsCancellationRequested) {
                    _client = await ReEstablishConnectionAsync().ConfigureAwait(false);
                }

                return _client?.GetStream();
            }
        }

        /// <summary>
        /// (Re-)establish the Kafka server connection.
        /// Assumes that the caller has already obtained the <c>_clientLock</c>
        /// </summary>
        private Task<TcpClient> ReEstablishConnectionAsync()
        {
            _log.Info(() => LogEvent.Create($"No connection to {Endpoint}: Attempting to connect..."));

            return _configuration.ConnectionRetry.AttemptAsync(
                async (attempt, timer) => {
                    _configuration.OnConnecting?.Invoke(Endpoint, attempt, timer.Elapsed);
                    var client = new TcpClient();

                    var connectTask = client.ConnectAsync(Endpoint.IP.Address, Endpoint.IP.Port);
                    if (await connectTask.IsCancelled(_disposeToken.Token).ConfigureAwait(false)) {
                        throw new ObjectDisposedException($"Object is disposing (TcpSocket for endpoint {Endpoint})");
                    }

                    await connectTask.ConfigureAwait(false);
                    if (!client.Connected) return RetryAttempt<TcpClient>.Retry;

                    _log.Info(() => LogEvent.Create($"Connection established to {Endpoint}"));
                    _configuration.OnConnected?.Invoke(Endpoint, attempt, timer.Elapsed);
                    return new RetryAttempt<TcpClient>(client);
                },
                (attempt, retry) => _log.Warn(() => LogEvent.Create($"Failed connection to {Endpoint}: Will retry in {retry}")),
                attempt => {
                    _log.Warn(() => LogEvent.Create($"Failed connection to {Endpoint} on attempt {attempt}"));
                    throw new ConnectionException(Endpoint);
                },
                (ex, attempt, retry) => _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {Endpoint}: Will retry in {retry}")),
                (ex, attempt) => _log.Warn(() => LogEvent.Create(ex, $"Failed connection to {Endpoint} on attempt {attempt}")),
                _disposeToken.Token);
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;
            _disposeToken.Cancel();

            using (_disposeToken) {
                using (_disposeRegistration) {
                    using (_client) { }
                }
            }
        }
    }
}