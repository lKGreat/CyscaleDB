using System.Threading.Channels;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;

namespace CysRedis.Core.Memory;

/// <summary>
/// Lazy free manager for asynchronous deletion of large objects.
/// Similar to Redis lazyfree.c - prevents blocking on large object deletion.
/// </summary>
public sealed class LazyFreeManager : IDisposable
{
    private readonly Channel<Action> _freeQueue;
    private readonly Task[] _workerTasks;
    private readonly CancellationTokenSource _cts;
    private const int FreeEffortThreshold = 64; // Objects with >64 elements go async
    private const int WorkerThreads = 2; // Number of background free threads
    private long _pendingFrees;
    private long _completedFrees;
    private bool _disposed;

    /// <summary>
    /// Number of pending async free operations.
    /// </summary>
    public long PendingFrees => Interlocked.Read(ref _pendingFrees);

    /// <summary>
    /// Number of completed async free operations.
    /// </summary>
    public long CompletedFrees => Interlocked.Read(ref _completedFrees);

    public LazyFreeManager()
    {
        _freeQueue = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false
        });
        _cts = new CancellationTokenSource();
        _workerTasks = new Task[WorkerThreads];

        // Start worker threads
        for (int i = 0; i < WorkerThreads; i++)
        {
            _workerTasks[i] = Task.Run(() => WorkerLoop(_cts.Token), _cts.Token);
        }

        Logger.Info("LazyFreeManager started with {0} worker threads", WorkerThreads);
    }

    /// <summary>
    /// Queues an object for lazy deletion.
    /// </summary>
    public void QueueFree(RedisObject obj)
    {
        var effort = EstimateFreeEffort(obj);

        if (effort > FreeEffortThreshold)
        {
            // Large object - async delete
            Interlocked.Increment(ref _pendingFrees);
            _freeQueue.Writer.TryWrite(() =>
            {
                FreeObject(obj);
                Interlocked.Decrement(ref _pendingFrees);
                Interlocked.Increment(ref _completedFrees);
            });
        }
        else
        {
            // Small object - sync delete
            FreeObject(obj);
        }
    }

    /// <summary>
    /// Queues a database flush for lazy deletion.
    /// </summary>
    public void QueueFlushDatabase(RedisDatabase db)
    {
        var keyCount = db.KeyCount;

        if (keyCount > FreeEffortThreshold)
        {
            Interlocked.Increment(ref _pendingFrees);
            _freeQueue.Writer.TryWrite(() =>
            {
                db.Flush();
                Interlocked.Decrement(ref _pendingFrees);
                Interlocked.Increment(ref _completedFrees);
            });
        }
        else
        {
            db.Flush();
        }
    }

    /// <summary>
    /// Estimates the free effort (complexity) of an object.
    /// Returns the approximate number of allocations that need to be freed.
    /// </summary>
    private int EstimateFreeEffort(RedisObject obj)
    {
        return obj switch
        {
            RedisList list => list.Count,
            RedisSet set => set.Count,
            RedisSortedSet zset => zset.Count,
            RedisHash hash => hash.Count,
            RedisStream stream => 100, // Streams are complex
            _ => 1
        };
    }

    /// <summary>
    /// Frees an object (synchronously).
    /// </summary>
    private void FreeObject(RedisObject obj)
    {
        if (obj is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    /// <summary>
    /// Worker loop for background free operations.
    /// </summary>
    private async Task WorkerLoop(CancellationToken cancellationToken)
    {
        await foreach (var freeAction in _freeQueue.Reader.ReadAllAsync(cancellationToken))
        {
            try
            {
                freeAction();
            }
            catch (Exception ex)
            {
                Logger.Error("LazyFree worker error: {0}", ex.Message);
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts.Cancel();
        _freeQueue.Writer.Complete();

        try
        {
            Task.WaitAll(_workerTasks, TimeSpan.FromSeconds(5));
        }
        catch (Exception)
        {
            // Ignore timeout
        }

        _cts.Dispose();
    }
}
