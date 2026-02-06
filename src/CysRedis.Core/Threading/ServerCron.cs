using System.Diagnostics;
using System.Runtime;
using CysRedis.Core.Memory;
using CysRedis.Core.Protocol;

namespace CysRedis.Core.Threading;

/// <summary>
/// Server periodic maintenance task similar to Redis serverCron.
/// Runs every 100ms to perform background housekeeping tasks.
/// </summary>
public sealed class ServerCron : IDisposable
{
    private readonly RedisServer _server;
    private const int IntervalMs = 100; // 100ms interval like Redis
    private const int CleanupKeysPerCycle = 20; // Keys to check per database per cycle
    private DateTime _lastCompaction = DateTime.MinValue;
    private DateTime _lastGcTune = DateTime.MinValue;
    private Task? _cronTask;
    private bool _disposed;

    private long _cyclesExecuted;
    private double _lastCycleDurationMs;

    /// <summary>
    /// Total cron cycles executed (thread-safe).
    /// </summary>
    public long CyclesExecuted => Interlocked.Read(ref _cyclesExecuted);

    /// <summary>
    /// Last cycle duration in milliseconds (thread-safe via volatile read).
    /// </summary>
    public double LastCycleDurationMs => Volatile.Read(ref _lastCycleDurationMs);

    public ServerCron(RedisServer server)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
    }

    /// <summary>
    /// Starts the server cron task.
    /// </summary>
    public void Start(CancellationToken cancellationToken)
    {
        _cronTask = Task.Run(async () =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var startTime = Stopwatch.GetTimestamp();

                try
                {
                    await ExecuteCycleAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    Common.Logger.Error("ServerCron cycle error: {0}", ex.Message);
                }

                Interlocked.Increment(ref _cyclesExecuted);

                // Calculate elapsed time and adjust delay
                var elapsedMs = (Stopwatch.GetTimestamp() - startTime) * 1000.0 / Stopwatch.Frequency;
                Volatile.Write(ref _lastCycleDurationMs, elapsedMs);

                var remainingMs = IntervalMs - (int)elapsedMs;
                if (remainingMs > 0)
                {
                    await Task.Delay(remainingMs, cancellationToken);
                }
            }
        }, cancellationToken);
    }

    /// <summary>
    /// Executes one cron cycle.
    /// </summary>
    private async Task ExecuteCycleAsync(CancellationToken cancellationToken)
    {
        // 1. Update LRU clock (already running in separate task, but ensure it's updated)
        LruClock.UpdateClock();

        // 2. Clean up expired keys (incremental, don't block)
        CleanupExpiredKeys();

        // 3. Trigger GC tuning if needed (every minute)
        if ((DateTime.UtcNow - _lastGcTune).TotalMinutes >= 1)
        {
            TuneGcIfNeeded();
            _lastGcTune = DateTime.UtcNow;
        }

                // 4. Update server statistics
                // Network metrics are updated automatically

        // 5. Cleanup expired hash fields
        CleanupExpiredHashFields();

        // 6. Trigger eviction if needed
        if (_server.Store.Eviction.NeedsEviction())
        {
            foreach (var db in _server.Store.GetAllDatabases())
            {
                _server.Store.Eviction.Evict(db, maxEvictions: 10);
            }
        }

        await Task.CompletedTask;
    }

    /// <summary>
    /// Incrementally clean up expired keys across all databases using probabilistic sampling.
    /// Uses Redis-style approach: sample N keys with TTL, delete expired ones.
    /// If expired ratio > 25%, sample again. Much more efficient than O(n) scan.
    /// </summary>
    private void CleanupExpiredKeys()
    {
        foreach (var db in _server.Store.GetAllDatabases())
        {
            // Use probabilistic sampling - sample 20 keys per cycle, up to 4 iterations
            db.CleanupExpired(sampleSize: CleanupKeysPerCycle, maxIterations: 4);
        }
    }

    /// <summary>
    /// Cleanup expired hash fields using sampling to avoid O(n) scan.
    /// </summary>
    private void CleanupExpiredHashFields()
    {
        foreach (var db in _server.Store.GetAllDatabases())
        {
            if (db.KeyCount == 0) continue;
            
            var cleaned = 0;
            var maxCleanup = CleanupKeysPerCycle;

            // Sample random keys and check if they are hashes with expiring fields
            var keys = db.GetRandomKeys(CleanupKeysPerCycle);
            foreach (var key in keys)
            {
                if (cleaned >= maxCleanup) break;

                try
                {
                    var hash = db.Get<DataStructures.RedisHash>(key);
                    if (hash != null)
                    {
                        cleaned += hash.CleanupExpiredFields();
                    }
                }
                catch (Common.WrongTypeException)
                {
                    // Not a hash, skip
                }
            }
        }
    }

    /// <summary>
    /// Tune GC settings based on memory pressure.
    /// </summary>
    private void TuneGcIfNeeded()
    {
        var memory = GC.GetTotalMemory(false);
        var maxMemory = _server.Options.MaxMemory;

        // Check memory pressure
        if (maxMemory > 0 && memory > maxMemory * 0.95)
        {
            // High memory pressure - trigger Gen1 collection
            GC.Collect(1, GCCollectionMode.Optimized, blocking: false);
            Common.Logger.Debug("Triggered Gen1 GC due to memory pressure");
        }

        // Periodic LOH compaction (every 10 minutes)
        var gen2Count = GC.CollectionCount(2);
        if (gen2Count > 100 && (DateTime.UtcNow - _lastCompaction).TotalMinutes >= 10)
        {
            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(2, GCCollectionMode.Optimized, blocking: false);
            _lastCompaction = DateTime.UtcNow;
            Common.Logger.Info("Triggered LOH compaction, Gen2 collections: {0}", gen2Count);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cronTask?.Wait(TimeSpan.FromSeconds(5));
    }
}
