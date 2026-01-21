using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Threading;

/// <summary>
/// Dispatches clients to I/O threads for processing.
/// Implements load balancing and thread affinity strategies.
/// </summary>
public sealed class ClientDispatcher : IDisposable
{
    private readonly IoThreadPool _threadPool;
    private readonly Dictionary<long, int> _clientThreadMap;
    private readonly object _lock = new();
    private bool _disposed;

    /// <summary>
    /// Gets whether multi-threaded I/O is enabled.
    /// </summary>
    public bool IsEnabled => _threadPool.IsEnabled;

    /// <summary>
    /// Gets the I/O thread pool.
    /// </summary>
    public IoThreadPool ThreadPool => _threadPool;

    /// <summary>
    /// Creates a new client dispatcher.
    /// </summary>
    /// <param name="options">I/O thread options.</param>
    /// <param name="commandHandler">Command handler delegate.</param>
    public ClientDispatcher(IoThreadOptions options, Func<RedisClient, string[], CancellationToken, Task>? commandHandler = null)
    {
        _threadPool = new IoThreadPool(options, commandHandler);
        _clientThreadMap = new Dictionary<long, int>();
    }

    /// <summary>
    /// Starts the dispatcher and all I/O threads.
    /// </summary>
    public void Start()
    {
        _threadPool.Start();
    }

    /// <summary>
    /// Assigns a client to an I/O thread.
    /// </summary>
    /// <param name="client">The client to assign.</param>
    /// <returns>The assigned thread ID, or -1 if not assigned.</returns>
    public int AssignClient(RedisClient client)
    {
        if (!IsEnabled)
            return -1;

        var thread = _threadPool.AssignClient(client);
        if (thread == null)
            return -1;

        lock (_lock)
        {
            _clientThreadMap[client.Id] = thread.Id;
        }

        return thread.Id;
    }

    /// <summary>
    /// Gets the I/O thread ID for a client.
    /// </summary>
    public int GetThreadId(long clientId)
    {
        lock (_lock)
        {
            return _clientThreadMap.TryGetValue(clientId, out var threadId) ? threadId : -1;
        }
    }

    /// <summary>
    /// Gets the I/O thread for a client.
    /// </summary>
    public IoThread? GetThread(RedisClient client)
    {
        var threadId = GetThreadId(client.Id);
        if (threadId < 0)
            return null;

        return _threadPool.GetThread(threadId);
    }

    /// <summary>
    /// Removes a client from the dispatcher.
    /// </summary>
    public void RemoveClient(long clientId)
    {
        if (!IsEnabled)
            return;

        int threadId;
        lock (_lock)
        {
            if (!_clientThreadMap.TryGetValue(clientId, out threadId))
                return;

            _clientThreadMap.Remove(clientId);
        }

        _threadPool.RemoveClient(clientId, threadId);
    }

    /// <summary>
    /// Queues a read operation for a client.
    /// </summary>
    public bool QueueRead(RedisClient client)
    {
        if (!IsEnabled)
            return false;

        var thread = GetThread(client);
        return _threadPool.QueueRead(client, thread);
    }

    /// <summary>
    /// Gets dispatcher statistics.
    /// </summary>
    public ClientDispatcherStats GetStats()
    {
        int mappedClients;
        lock (_lock)
        {
            mappedClients = _clientThreadMap.Count;
        }

        return new ClientDispatcherStats
        {
            Enabled = IsEnabled,
            MappedClients = mappedClients,
            ThreadPoolStats = _threadPool.GetStats()
        };
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _threadPool.Dispose();
        }
    }
}

/// <summary>
/// Client dispatcher statistics.
/// </summary>
public sealed class ClientDispatcherStats
{
    public bool Enabled { get; init; }
    public int MappedClients { get; init; }
    public IoThreadPoolStats ThreadPoolStats { get; init; } = new();

    public override string ToString()
    {
        return $"ClientDispatcher: enabled={Enabled}, clients={MappedClients}";
    }
}
