using System.Runtime.CompilerServices;

namespace CysRedis.Core.Memory;

/// <summary>
/// Multi-threaded memory usage tracker similar to Redis zmalloc.
/// Uses per-thread counters to avoid lock contention.
/// </summary>
public static class MemoryTracker
{
    // Per-thread memory counters (cache-line aligned to prevent false sharing)
    private const int CacheLineSize = 64;
    private const int MaxThreads = 128;
    private static readonly long[] _perThreadMemory;
    private static int _activeThreads;
    
    // Thread-local index
    [ThreadStatic] private static int _threadIndex = -1;

    static MemoryTracker()
    {
        // Allocate aligned array for per-thread counters
        _perThreadMemory = new long[MaxThreads * (CacheLineSize / sizeof(long))];
        _activeThreads = 0;
    }

    /// <summary>
    /// Gets the current thread's index in the memory array.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetThreadIndex()
    {
        if (_threadIndex == -1)
        {
            _threadIndex = Interlocked.Increment(ref _activeThreads) % MaxThreads;
        }
        return _threadIndex;
    }

    /// <summary>
    /// Gets the array offset for a thread's memory counter.
    /// Ensures cache-line alignment to prevent false sharing.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetArrayOffset(int threadIndex)
    {
        return threadIndex * (CacheLineSize / sizeof(long));
    }

    /// <summary>
    /// Tracks memory allocation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void TrackAllocation(long size)
    {
        var threadIndex = GetThreadIndex();
        var offset = GetArrayOffset(threadIndex);
        Interlocked.Add(ref _perThreadMemory[offset], size);
    }

    /// <summary>
    /// Tracks memory deallocation.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void TrackDeallocation(long size)
    {
        var threadIndex = GetThreadIndex();
        var offset = GetArrayOffset(threadIndex);
        Interlocked.Add(ref _perThreadMemory[offset], -size);
    }

    /// <summary>
    /// Gets the total tracked memory usage across all threads.
    /// </summary>
    public static long GetTrackedMemory()
    {
        long total = 0;
        for (int i = 0; i < MaxThreads; i++)
        {
            var offset = GetArrayOffset(i);
            total += Interlocked.Read(ref _perThreadMemory[offset]);
        }
        return total;
    }

    /// <summary>
    /// Gets the actual GC memory usage.
    /// </summary>
    public static long GetGcMemory(bool forceFullCollection = false)
    {
        return GC.GetTotalMemory(forceFullCollection);
    }

    /// <summary>
    /// Synchronizes tracked memory with actual GC memory.
    /// Call this periodically to correct drift.
    /// </summary>
    public static void SyncWithGC()
    {
        var gcMemory = GetGcMemory(false);
        var trackedMemory = GetTrackedMemory();
        var drift = Math.Abs(gcMemory - trackedMemory);

        // If drift is significant (>10%), log it
        if (drift > gcMemory * 0.1)
        {
            Common.Logger.Debug("Memory tracking drift: tracked={0}, actual={1}, drift={2:P}",
                trackedMemory, gcMemory, (double)drift / gcMemory);
        }
    }

    /// <summary>
    /// Resets all memory counters.
    /// </summary>
    public static void Reset()
    {
        for (int i = 0; i < MaxThreads; i++)
        {
            var offset = GetArrayOffset(i);
            Interlocked.Exchange(ref _perThreadMemory[offset], 0);
        }
    }

    /// <summary>
    /// Gets memory statistics.
    /// </summary>
    public static MemoryStats GetStats()
    {
        return new MemoryStats
        {
            TrackedMemory = GetTrackedMemory(),
            GcTotalMemory = GetGcMemory(false),
            Gen0Collections = GC.CollectionCount(0),
            Gen1Collections = GC.CollectionCount(1),
            Gen2Collections = GC.CollectionCount(2),
            ActiveThreads = _activeThreads
        };
    }
}

/// <summary>
/// Memory statistics.
/// </summary>
public sealed class MemoryStats
{
    public long TrackedMemory { get; init; }
    public long GcTotalMemory { get; init; }
    public int Gen0Collections { get; init; }
    public int Gen1Collections { get; init; }
    public int Gen2Collections { get; init; }
    public int ActiveThreads { get; init; }

    public override string ToString()
    {
        return $"Memory: tracked={TrackedMemory:N0}, gc={GcTotalMemory:N0}, " +
               $"collections=[{Gen0Collections}/{Gen1Collections}/{Gen2Collections}], threads={ActiveThreads}";
    }
}
