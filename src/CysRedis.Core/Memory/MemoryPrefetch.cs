using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using CysRedis.Core.DataStructures;

namespace CysRedis.Core.Memory;

/// <summary>
/// Memory prefetching utilities to improve cache locality.
/// Uses CPU prefetch instructions (SSE/AVX) when available.
/// </summary>
public static class MemoryPrefetch
{
    /// <summary>
    /// Prefetches a memory location into L1 cache (read-only).
    /// Uses SSE PREFETCH0 instruction when available.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void PrefetchRead(void* address)
    {
        if (Sse.IsSupported)
        {
            // PREFETCHT0 - prefetch to L1 cache (temporal data)
            Sse.Prefetch0(address);
        }
    }

    /// <summary>
    /// Prefetches a memory location into L1 cache (read-write).
    /// Uses SSE PREFETCH1 instruction when available.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void PrefetchWrite(void* address)
    {
        if (Sse.IsSupported)
        {
            // PREFETCHT1 - prefetch to L2 cache for write access
            Sse.Prefetch1(address);
        }
    }
    
    /// <summary>
    /// Prefetches a memory location from a managed pointer.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe void PrefetchObject<T>(T obj) where T : class
    {
        if (Sse.IsSupported && obj != null)
        {
            // Get pointer to object and prefetch
            fixed (void* ptr = &System.Runtime.CompilerServices.Unsafe.AsRef(obj))
            {
                Sse.Prefetch0(ptr);
            }
        }
    }

    /// <summary>
    /// Prefetches keys for a batch of operations (Pipeline mode).
    /// Uses parallel lookups to trigger cache loading.
    /// </summary>
    public static void PrefetchKeys(RedisDatabase db, string[] keys)
    {
        if (!ShouldPrefetch(keys.Length))
            return;

        // Phase 1: Parallel existence check to warm up dictionary buckets
        // This triggers concurrent dictionary access which loads relevant buckets into CPU cache
        Parallel.For(0, keys.Length, new ParallelOptions { MaxDegreeOfParallelism = 4 }, i =>
        {
            _ = db.Exists(keys[i]);
        });
    }

    /// <summary>
    /// Prefetches keys with values for a batch of GET operations.
    /// Uses two-phase prefetching for optimal cache utilization.
    /// </summary>
    public static void PrefetchKeysWithValues(RedisDatabase db, string[] keys)
    {
        if (!ShouldPrefetch(keys.Length))
            return;

        // Phase 1: Parallel key existence check (loads dictionary buckets)
        Parallel.For(0, keys.Length, new ParallelOptions { MaxDegreeOfParallelism = 4 }, i =>
        {
            _ = db.Exists(keys[i]);
        });

        // Small delay to let prefetch complete
        System.Threading.Thread.SpinWait(100);

        // Phase 2: Parallel value retrieval (data should be in cache now)
        Parallel.For(0, keys.Length, new ParallelOptions { MaxDegreeOfParallelism = 4 }, i =>
        {
            var obj = db.Get(keys[i]);
            if (obj != null)
            {
                // Access object to ensure it's in cache
                PrefetchObject(obj);
            }
        });
    }

    /// <summary>
    /// Prefetches data for MGET command with optimized cache access pattern.
    /// </summary>
    public static RedisObject?[] PrefetchMGet(RedisDatabase db, string[] keys)
    {
        var results = new RedisObject?[keys.Length];

        if (!ShouldPrefetch(keys.Length))
        {
            // For small batches, just fetch normally
            for (int i = 0; i < keys.Length; i++)
            {
                results[i] = db.Get(keys[i]);
            }
            return results;
        }

        // For larger batches, use two-phase prefetching
        // Phase 1: Parallel prefetch (trigger dictionary lookups)
        Parallel.For(0, keys.Length, new ParallelOptions { MaxDegreeOfParallelism = 4 }, i =>
        {
            _ = db.Exists(keys[i]);
        });

        // Small spin wait to let CPU prefetch settle
        System.Threading.Thread.SpinWait(50);

        // Phase 2: Sequential retrieval (data should be in cache)
        for (int i = 0; i < keys.Length; i++)
        {
            results[i] = db.Get(keys[i]);
            
            // Prefetch next key while processing current
            if (i + 1 < keys.Length)
            {
                _ = db.Exists(keys[i + 1]);
            }
        }

        return results;
    }
    
    /// <summary>
    /// Optimized batch key lookup with interleaved prefetching.
    /// Processes keys in a pipelined fashion to maximize cache hits.
    /// </summary>
    public static void PrefetchBatchLookup(RedisDatabase db, string[] keys, Action<string, RedisObject?> processAction)
    {
        if (keys.Length == 0) return;

        if (!ShouldPrefetch(keys.Length))
        {
            // Small batch - process directly
            foreach (var key in keys)
            {
                var obj = db.Get(key);
                processAction(key, obj);
            }
            return;
        }

        // Large batch - use prefetching pipeline
        const int PrefetchWindow = 8; // Prefetch up to 8 keys ahead

        for (int i = 0; i < keys.Length; i++)
        {
            // Prefetch ahead
            for (int j = i + 1; j < Math.Min(i + PrefetchWindow, keys.Length); j++)
            {
                _ = db.Exists(keys[j]); // Trigger prefetch
            }

            // Process current key (should be in cache from previous prefetch)
            var obj = db.Get(keys[i]);
            processAction(keys[i], obj);
        }
    }

    /// <summary>
    /// Estimates whether prefetching would be beneficial.
    /// </summary>
    public static bool ShouldPrefetch(int batchSize)
    {
        // Prefetching only helps for batches >= 4 items
        return batchSize >= 4;
    }
}
