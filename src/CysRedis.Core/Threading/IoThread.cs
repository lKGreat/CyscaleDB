using System.Collections.Concurrent;
using System.Threading.Channels;
using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Threading;

/// <summary>
/// Represents a pending client operation.
/// </summary>
public sealed class ClientOperation
{
    public RedisClient Client { get; }
    public string[]? Command { get; }
    public bool IsReadComplete { get; set; }
    public bool IsWriteComplete { get; set; }
    public TaskCompletionSource? CompletionSource { get; set; }

    public ClientOperation(RedisClient client, string[]? command = null)
    {
        Client = client;
        Command = command;
    }
}

/// <summary>
/// Individual I/O thread for handling client reads and writes.
/// Based on Redis 8.x io_threads model.
/// </summary>
public sealed class IoThread : IDisposable
{
    private readonly int _id;
    private readonly Thread _thread;
    private readonly CancellationTokenSource _cts;
    private readonly Channel<RedisClient> _readQueue;
    private readonly Channel<ClientOperation> _writeQueue;
    private readonly ConcurrentDictionary<long, RedisClient> _assignedClients;
    private readonly Func<RedisClient, string[], CancellationToken, Task>? _commandHandler;
    private readonly ManualResetEventSlim _wakeEvent = new(false); // Event-driven wakeup
    private bool _disposed;

    // Statistics
    private long _readOperations;
    private long _writeOperations;
    private long _errors;

    /// <summary>
    /// Gets the thread ID.
    /// </summary>
    public int Id => _id;

    /// <summary>
    /// Gets the number of assigned clients.
    /// </summary>
    public int ClientCount => _assignedClients.Count;

    /// <summary>
    /// Gets the number of read operations processed.
    /// </summary>
    public long ReadOperations => Interlocked.Read(ref _readOperations);

    /// <summary>
    /// Gets the number of write operations processed.
    /// </summary>
    public long WriteOperations => Interlocked.Read(ref _writeOperations);

    /// <summary>
    /// Gets the number of errors.
    /// </summary>
    public long Errors => Interlocked.Read(ref _errors);

    /// <summary>
    /// Creates a new I/O thread.
    /// </summary>
    /// <param name="id">Thread identifier.</param>
    /// <param name="commandHandler">Command handler delegate.</param>
    public IoThread(int id, Func<RedisClient, string[], CancellationToken, Task>? commandHandler = null)
    {
        _id = id;
        _cts = new CancellationTokenSource();
        _readQueue = Channel.CreateBounded<RedisClient>(new BoundedChannelOptions(10000)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest // Backpressure: drop oldest if queue is full
        });
        _writeQueue = Channel.CreateBounded<ClientOperation>(new BoundedChannelOptions(10000)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait // Wait for space on write queue
        });
        _assignedClients = new ConcurrentDictionary<long, RedisClient>();
        _commandHandler = commandHandler;

        _thread = new Thread(RunLoop)
        {
            Name = $"CysRedis-IO-{id}",
            IsBackground = true,
            Priority = ThreadPriority.AboveNormal
        };
    }

    /// <summary>
    /// Starts the I/O thread.
    /// </summary>
    public void Start()
    {
        _thread.Start();
        Logger.Debug("I/O Thread {0} started", _id);
    }

    /// <summary>
    /// Assigns a client to this I/O thread.
    /// </summary>
    public void AssignClient(RedisClient client)
    {
        _assignedClients[client.Id] = client;
        Logger.Debug("Client {0} assigned to I/O thread {1}", client.Id, _id);
    }

    /// <summary>
    /// Removes a client from this I/O thread.
    /// </summary>
    public void RemoveClient(long clientId)
    {
        _assignedClients.TryRemove(clientId, out _);
    }

    /// <summary>
    /// Queues a read operation for a client and wakes the I/O thread.
    /// </summary>
    public bool QueueRead(RedisClient client)
    {
        var result = _readQueue.Writer.TryWrite(client);
        if (result)
            _wakeEvent.Set(); // Signal the thread to wake up
        return result;
    }

    /// <summary>
    /// Queues a write operation for a client and wakes the I/O thread.
    /// </summary>
    public bool QueueWrite(ClientOperation operation)
    {
        var result = _writeQueue.Writer.TryWrite(operation);
        if (result)
            _wakeEvent.Set(); // Signal the thread to wake up
        return result;
    }

    /// <summary>
    /// Main thread loop using event-driven wakeup to avoid CPU-wasting Thread.Sleep(1).
    /// Uses SpinWait for short spins then blocks on ManualResetEventSlim.
    /// </summary>
    private void RunLoop()
    {
        var spinWait = new SpinWait();

        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                bool hadWork = false;

                // Process reads first (higher priority)
                if (_readQueue.Reader.TryRead(out _) == false)
                {
                    // Peek is not available, so try processing reads
                }
                hadWork |= ProcessReads();

                // Then process writes
                hadWork |= ProcessWrites();

                if (!hadWork)
                {
                    // No work: use SpinWait for short burst, then block on event
                    if (spinWait.NextSpinWillYield)
                    {
                        // Block until new work arrives or timeout (100ms max)
                        _wakeEvent.Wait(100, _cts.Token);
                        _wakeEvent.Reset();
                        spinWait.Reset();
                    }
                    else
                    {
                        spinWait.SpinOnce();
                    }
                }
                else
                {
                    spinWait.Reset();
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _errors);
                Logger.Error("Error in I/O thread {0}", _id, ex);
            }
        }

        Logger.Debug("I/O Thread {0} stopped", _id);
    }

    /// <summary>
    /// Processes pending read operations.
    /// Returns true if any work was done.
    /// </summary>
    private bool ProcessReads()
    {
        bool hadWork = false;
        while (_readQueue.Reader.TryRead(out var client))
        {
            hadWork = true;
            try
            {
                // Read command from client (non-blocking if possible)
                var readTask = client.ReadCommandAsync(_cts.Token);
                if (readTask.IsCompleted)
                {
                    var args = readTask.Result;
                    if (args != null && args.Length > 0 && _commandHandler != null)
                    {
                        // Queue for command execution
                        _commandHandler(client, args, _cts.Token);
                    }
                    Interlocked.Increment(ref _readOperations);
                }
                else
                {
                    // If not immediately complete, wait async
                    _ = ProcessReadAsync(client, readTask);
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _errors);
                Logger.Error("Error reading from client {0} on I/O thread {1}",
                    client.Id, _id, ex);
            }
        }
        return hadWork;
    }

    /// <summary>
    /// Processes a read operation asynchronously.
    /// </summary>
    private async Task ProcessReadAsync(RedisClient client, Task<string[]?> readTask)
    {
        try
        {
            var args = await readTask.ConfigureAwait(false);
            if (args != null && args.Length > 0 && _commandHandler != null)
            {
                await _commandHandler(client, args, _cts.Token).ConfigureAwait(false);
            }
            Interlocked.Increment(ref _readOperations);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _errors);
            Logger.Error("Error in async read for client {0}", client.Id, ex);
        }
    }

    /// <summary>
    /// Processes pending write operations.
    /// Returns true if any work was done.
    /// </summary>
    private bool ProcessWrites()
    {
        bool hadWork = false;
        while (_writeQueue.Reader.TryRead(out var operation))
        {
            hadWork = true;
            try
            {
                operation.IsWriteComplete = true;
                operation.CompletionSource?.TrySetResult();
                Interlocked.Increment(ref _writeOperations);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _errors);
                operation.CompletionSource?.TrySetException(ex);
                Logger.Error("Error writing to client {0} on I/O thread {1}",
                    operation.Client.Id, _id, ex);
            }
        }
        return hadWork;
    }

    /// <summary>
    /// Stops the I/O thread.
    /// </summary>
    public void Stop()
    {
        _cts.Cancel();
        _wakeEvent.Set(); // Wake the thread so it can exit
        _readQueue.Writer.TryComplete();
        _writeQueue.Writer.TryComplete();

        if (!_thread.Join(TimeSpan.FromSeconds(5)))
        {
            Logger.Warning("I/O thread {0} did not stop gracefully", _id);
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            Stop();
            _cts.Dispose();
            _wakeEvent.Dispose();
        }
    }
}
