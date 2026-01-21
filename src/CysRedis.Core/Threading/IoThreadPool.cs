using CysRedis.Core.Common;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Threading;

/// <summary>
/// I/O thread pool configuration.
/// </summary>
public class IoThreadOptions
{
    /// <summary>
    /// Number of I/O threads. 0 = auto (based on CPU cores), 1 = disabled (single-threaded).
    /// </summary>
    public int IoThreads { get; set; } = 0;

    /// <summary>
    /// Whether I/O threading is enabled for reads.
    /// </summary>
    public bool EnableReadThreads { get; set; } = true;

    /// <summary>
    /// Whether I/O threading is enabled for writes.
    /// </summary>
    public bool EnableWriteThreads { get; set; } = true;

    /// <summary>
    /// Minimum clients required to activate I/O threads.
    /// </summary>
    public int ActivationThreshold { get; set; } = 4;
}

/// <summary>
/// Manages a pool of I/O threads for parallel client processing.
/// Based on Redis 8.x threaded I/O model.
/// </summary>
public sealed class IoThreadPool : IDisposable
{
    private readonly IoThread[] _threads;
    private readonly IoThreadOptions _options;
    private readonly Func<RedisClient, string[], CancellationToken, Task>? _commandHandler;
    private int _nextThreadIndex;
    private bool _disposed;
    private bool _started;

    /// <summary>
    /// Gets whether the thread pool is enabled.
    /// </summary>
    public bool IsEnabled => _threads.Length > 0 && _options.IoThreads > 1;

    /// <summary>
    /// Gets the number of I/O threads.
    /// </summary>
    public int ThreadCount => _threads.Length;

    /// <summary>
    /// Gets the I/O thread options.
    /// </summary>
    public IoThreadOptions Options => _options;

    /// <summary>
    /// Creates a new I/O thread pool.
    /// </summary>
    /// <param name="options">Thread pool options.</param>
    /// <param name="commandHandler">Command handler delegate.</param>
    public IoThreadPool(IoThreadOptions options, Func<RedisClient, string[], CancellationToken, Task>? commandHandler = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _commandHandler = commandHandler;

        // Determine thread count
        var threadCount = _options.IoThreads;
        if (threadCount == 0)
        {
            // Auto: use number of CPU cores - 1, minimum 2
            threadCount = Math.Max(2, Environment.ProcessorCount - 1);
        }

        if (threadCount <= 1)
        {
            // Disabled - use empty array
            _threads = Array.Empty<IoThread>();
            Logger.Info("I/O thread pool disabled (single-threaded mode)");
            return;
        }

        // Create I/O threads
        _threads = new IoThread[threadCount];
        for (int i = 0; i < threadCount; i++)
        {
            _threads[i] = new IoThread(i, commandHandler);
        }

        Logger.Info("I/O thread pool initialized with {0} threads", threadCount);
    }

    /// <summary>
    /// Starts all I/O threads.
    /// </summary>
    public void Start()
    {
        if (_started || !IsEnabled)
            return;

        _started = true;
        foreach (var thread in _threads)
        {
            thread.Start();
        }
    }

    /// <summary>
    /// Assigns a client to an I/O thread.
    /// Uses round-robin assignment for load balancing.
    /// </summary>
    /// <param name="client">The client to assign.</param>
    /// <returns>The assigned I/O thread, or null if threading is disabled.</returns>
    public IoThread? AssignClient(RedisClient client)
    {
        if (!IsEnabled)
            return null;

        var threadIndex = Interlocked.Increment(ref _nextThreadIndex) % _threads.Length;
        var thread = _threads[threadIndex];
        thread.AssignClient(client);

        return thread;
    }

    /// <summary>
    /// Removes a client from its assigned I/O thread.
    /// </summary>
    /// <param name="clientId">The client ID.</param>
    /// <param name="threadId">The thread ID.</param>
    public void RemoveClient(long clientId, int threadId)
    {
        if (!IsEnabled || threadId < 0 || threadId >= _threads.Length)
            return;

        _threads[threadId].RemoveClient(clientId);
    }

    /// <summary>
    /// Gets the I/O thread at the specified index.
    /// </summary>
    public IoThread? GetThread(int index)
    {
        if (!IsEnabled || index < 0 || index >= _threads.Length)
            return null;

        return _threads[index];
    }

    /// <summary>
    /// Queues a read operation for a client.
    /// </summary>
    public bool QueueRead(RedisClient client, IoThread? thread)
    {
        if (!IsEnabled || !_options.EnableReadThreads || thread == null)
            return false;

        return thread.QueueRead(client);
    }

    /// <summary>
    /// Gets statistics for all I/O threads.
    /// </summary>
    public IoThreadPoolStats GetStats()
    {
        var threadStats = new List<IoThreadStats>();
        long totalReadOps = 0;
        long totalWriteOps = 0;
        long totalErrors = 0;

        foreach (var thread in _threads)
        {
            var stats = new IoThreadStats
            {
                Id = thread.Id,
                ClientCount = thread.ClientCount,
                ReadOperations = thread.ReadOperations,
                WriteOperations = thread.WriteOperations,
                Errors = thread.Errors
            };
            threadStats.Add(stats);
            totalReadOps += stats.ReadOperations;
            totalWriteOps += stats.WriteOperations;
            totalErrors += stats.Errors;
        }

        return new IoThreadPoolStats
        {
            Enabled = IsEnabled,
            ThreadCount = _threads.Length,
            TotalReadOperations = totalReadOps,
            TotalWriteOperations = totalWriteOps,
            TotalErrors = totalErrors,
            ThreadStats = threadStats.ToArray()
        };
    }

    /// <summary>
    /// Stops all I/O threads.
    /// </summary>
    public void Stop()
    {
        foreach (var thread in _threads)
        {
            thread.Stop();
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            Stop();
            foreach (var thread in _threads)
            {
                thread.Dispose();
            }
        }
    }
}

/// <summary>
/// Statistics for a single I/O thread.
/// </summary>
public sealed class IoThreadStats
{
    public int Id { get; init; }
    public int ClientCount { get; init; }
    public long ReadOperations { get; init; }
    public long WriteOperations { get; init; }
    public long Errors { get; init; }
}

/// <summary>
/// Statistics for the I/O thread pool.
/// </summary>
public sealed class IoThreadPoolStats
{
    public bool Enabled { get; init; }
    public int ThreadCount { get; init; }
    public long TotalReadOperations { get; init; }
    public long TotalWriteOperations { get; init; }
    public long TotalErrors { get; init; }
    public IoThreadStats[] ThreadStats { get; init; } = Array.Empty<IoThreadStats>();

    public override string ToString()
    {
        return $"IoThreadPool: {ThreadCount} threads, reads={TotalReadOperations}, writes={TotalWriteOperations}";
    }
}
