using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using CysRedis.Core.Unsafe.Common;

namespace CysRedis.Core.Unsafe.Common;

/// <summary>
/// High-performance buffer pool with cache-line aligned memory allocation.
/// Provides thread-local caching to avoid lock contention.
/// </summary>
public static unsafe class UnsafeBufferPool
{
    private const int CacheLineSize = SimdHelpers.CacheLineSize;
    private const int MaxPooledSize = 64 * 1024; // 64KB max
    
    // Thread-local pool to avoid contention
    [ThreadStatic]
    private static ThreadLocalPool? _threadPool;
    
    // Global pools for larger buffers
    private static readonly ConcurrentQueue<PooledBuffer>[] _globalPools = 
        new ConcurrentQueue<PooledBuffer>[32]; // 32 size buckets
    
    static UnsafeBufferPool()
    {
        for (int i = 0; i < _globalPools.Length; i++)
        {
            _globalPools[i] = new ConcurrentQueue<PooledBuffer>();
        }
    }
    
    /// <summary>
    /// Rents a cache-line aligned buffer of at least the specified size.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PooledBuffer Rent(int minimumLength)
    {
        if (minimumLength <= 0)
            minimumLength = CacheLineSize;
        
        // Round up to cache line size
        int alignedSize = (minimumLength + CacheLineSize - 1) & ~(CacheLineSize - 1);
        
        // Try thread-local pool first
        var threadPool = _threadPool ??= new ThreadLocalPool();
        if (threadPool.TryRent(alignedSize, out var buffer))
        {
            return buffer;
        }
        
        // Try global pool
        int bucketIndex = GetBucketIndex(alignedSize);
        if (bucketIndex < _globalPools.Length && _globalPools[bucketIndex].TryDequeue(out var globalBuffer))
        {
            if (globalBuffer.Size >= alignedSize)
            {
                return globalBuffer;
            }
            // Return to pool if too small
            Return(globalBuffer);
        }
        
        // Allocate new buffer
        void* ptr = UnsafeMemoryManager.AlignedAlloc((nuint)alignedSize, CacheLineSize);
        return new PooledBuffer(ptr, alignedSize);
    }
    
    /// <summary>
    /// Returns a buffer to the pool.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(PooledBuffer buffer)
    {
        if (buffer.Pointer == null || buffer.Size == 0)
            return;
        
        if (buffer.Size > MaxPooledSize)
        {
            // Too large, free immediately
            UnsafeMemoryManager.AlignedFree(buffer.Pointer);
            return;
        }
        
        // Try thread-local pool first
        var threadPool = _threadPool ??= new ThreadLocalPool();
        if (threadPool.TryReturn(buffer))
        {
            return;
        }
        
        // Return to global pool
        int bucketIndex = GetBucketIndex(buffer.Size);
        if (bucketIndex < _globalPools.Length)
        {
            _globalPools[bucketIndex].Enqueue(buffer);
        }
        else
        {
            // Free if can't pool
            UnsafeMemoryManager.AlignedFree(buffer.Pointer);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetBucketIndex(int size)
    {
        // Use log2 to determine bucket
        int log2 = 0;
        int temp = size;
        while (temp > 1)
        {
            temp >>= 1;
            log2++;
        }
        return Math.Min(log2, _globalPools.Length - 1);
    }
    
    /// <summary>
    /// Thread-local buffer pool.
    /// </summary>
    private class ThreadLocalPool
    {
        private const int MaxBuffers = 16;
        private readonly PooledBuffer[] _buffers = new PooledBuffer[MaxBuffers];
        private int _count;
        
        public bool TryRent(int size, out PooledBuffer buffer)
        {
            for (int i = 0; i < _count; i++)
            {
                if (_buffers[i].Size >= size)
                {
                    buffer = _buffers[i];
                    // Remove from pool
                    _buffers[i] = _buffers[_count - 1];
                    _count--;
                    return true;
                }
            }
            
            buffer = default;
            return false;
        }
        
        public bool TryReturn(PooledBuffer buffer)
        {
            if (_count >= MaxBuffers)
                return false;
            
            _buffers[_count++] = buffer;
            return true;
        }
    }
}

/// <summary>
/// Represents a pooled buffer that must be returned.
/// </summary>
public unsafe struct PooledBuffer : IDisposable
{
    private void* _pointer;
    private int _size;
    
    /// <summary>
    /// Gets the pointer to the buffer.
    /// </summary>
    public void* Pointer => _pointer;
    
    /// <summary>
    /// Gets the size of the buffer.
    /// </summary>
    public int Size => _size;
    
    /// <summary>
    /// Gets a span over the buffer.
    /// </summary>
    public Span<byte> Span => new Span<byte>(_pointer, _size);
    
    internal PooledBuffer(void* pointer, int size)
    {
        _pointer = pointer;
        _size = size;
    }
    
    /// <summary>
    /// Returns the buffer to the pool.
    /// </summary>
    public void Dispose()
    {
        if (_pointer != null)
        {
            UnsafeBufferPool.Return(this);
            _pointer = null;
            _size = 0;
        }
    }
}
