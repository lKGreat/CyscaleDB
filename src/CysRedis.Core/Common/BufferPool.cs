using System.Buffers;
using System.Collections.Concurrent;

namespace CysRedis.Core.Common;

/// <summary>
/// Shared buffer pools for memory-efficient I/O operations.
/// </summary>
public static class BufferPool
{
    /// <summary>
    /// Default shared array pool for general use.
    /// </summary>
    public static ArrayPool<byte> Shared => ArrayPool<byte>.Shared;

    /// <summary>
    /// Pool for large buffers (up to 1MB).
    /// Used for bulk string operations and large data transfers.
    /// </summary>
    public static readonly ArrayPool<byte> Large = ArrayPool<byte>.Create(
        maxArrayLength: 1 * 1024 * 1024,  // 1MB max
        maxArraysPerBucket: 16
    );

    /// <summary>
    /// Pool for small buffers (up to 4KB).
    /// Used for command parsing and small responses.
    /// </summary>
    public static readonly ArrayPool<byte> Small = ArrayPool<byte>.Create(
        maxArrayLength: 4 * 1024,  // 4KB max
        maxArraysPerBucket: 64
    );

    /// <summary>
    /// Rents a buffer of at least the specified size.
    /// </summary>
    /// <param name="minimumLength">Minimum required length.</param>
    /// <returns>A pooled buffer that must be returned using Return().</returns>
    public static byte[] Rent(int minimumLength)
    {
        if (minimumLength <= 4 * 1024)
            return Small.Rent(minimumLength);
        if (minimumLength <= 1 * 1024 * 1024)
            return Large.Rent(minimumLength);
        return Shared.Rent(minimumLength);
    }

    /// <summary>
    /// Returns a rented buffer to the pool.
    /// </summary>
    /// <param name="buffer">The buffer to return.</param>
    /// <param name="clearArray">Whether to clear the buffer contents.</param>
    public static void Return(byte[] buffer, bool clearArray = false)
    {
        if (buffer.Length <= 4 * 1024)
            Small.Return(buffer, clearArray);
        else if (buffer.Length <= 1 * 1024 * 1024)
            Large.Return(buffer, clearArray);
        else
            Shared.Return(buffer, clearArray);
    }

    /// <summary>
    /// Creates a MemoryPool that wraps the shared ArrayPool.
    /// </summary>
    public static MemoryPool<byte> CreateMemoryPool() => new ArrayPoolMemoryPool();

    /// <summary>
    /// Memory pool wrapper around ArrayPool.
    /// </summary>
    private sealed class ArrayPoolMemoryPool : MemoryPool<byte>
    {
        public override int MaxBufferSize => int.MaxValue;

        public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
        {
            if (minBufferSize == -1)
                minBufferSize = 4096;
            return new ArrayPoolMemoryOwner(BufferPool.Rent(minBufferSize), minBufferSize);
        }

        protected override void Dispose(bool disposing) { }
    }

    /// <summary>
    /// Memory owner for pooled arrays.
    /// </summary>
    private sealed class ArrayPoolMemoryOwner : IMemoryOwner<byte>
    {
        private byte[]? _array;
        private readonly int _length;

        public ArrayPoolMemoryOwner(byte[] array, int length)
        {
            _array = array;
            _length = length;
        }

        public Memory<byte> Memory
        {
            get
            {
                var array = _array;
                if (array == null)
                    throw new ObjectDisposedException(nameof(ArrayPoolMemoryOwner));
                return new Memory<byte>(array, 0, _length);
            }
        }

        public void Dispose()
        {
            var array = _array;
            if (array != null)
            {
                _array = null;
                BufferPool.Return(array);
            }
        }
    }
}

/// <summary>
/// RAII-style buffer rental that automatically returns the buffer on dispose.
/// </summary>
public readonly struct RentedBuffer : IDisposable
{
    private readonly byte[] _buffer;
    private readonly int _length;

    /// <summary>
    /// The rented buffer.
    /// </summary>
    public byte[] Array => _buffer;

    /// <summary>
    /// The requested length (may be smaller than Array.Length).
    /// </summary>
    public int Length => _length;

    /// <summary>
    /// Gets a span over the requested length.
    /// </summary>
    public Span<byte> Span => _buffer.AsSpan(0, _length);

    /// <summary>
    /// Gets a memory over the requested length.
    /// </summary>
    public Memory<byte> Memory => _buffer.AsMemory(0, _length);

    public RentedBuffer(int minimumLength)
    {
        _buffer = BufferPool.Rent(minimumLength);
        _length = minimumLength;
    }

    public void Dispose()
    {
        BufferPool.Return(_buffer);
    }
}

/// <summary>
/// Interface for objects that can be reset to their initial state for reuse.
/// </summary>
public interface IResettable
{
    /// <summary>
    /// Resets the object to its initial state for reuse.
    /// </summary>
    void Reset();
}

/// <summary>
/// Generic object pool for reducing GC pressure.
/// </summary>
public static class ObjectPool<T> where T : class, new()
{
    private static readonly ConcurrentQueue<T> _pool = new();
    private const int MaxPoolSize = 1024;
    private static int _poolCount;
    
    /// <summary>
    /// Rents an object from the pool, or creates a new one if none available.
    /// </summary>
    public static T Rent()
    {
        if (_pool.TryDequeue(out var obj))
        {
            Interlocked.Decrement(ref _poolCount);
            return obj;
        }
        return new T();
    }
    
    /// <summary>
    /// Returns an object to the pool for reuse.
    /// </summary>
    public static void Return(T obj)
    {
        if (obj == null) return;
        
        // Reset object state if it implements IResettable
        if (obj is IResettable resettable)
        {
            resettable.Reset();
        }
        
        // Don't grow the pool beyond MaxPoolSize
        if (_poolCount < MaxPoolSize)
        {
            _pool.Enqueue(obj);
            Interlocked.Increment(ref _poolCount);
        }
    }
    
    /// <summary>
    /// Gets the current pool size.
    /// </summary>
    public static int PoolSize => _poolCount;
    
    /// <summary>
    /// Clears all objects from the pool.
    /// </summary>
    public static void Clear()
    {
        while (_pool.TryDequeue(out _))
        {
            Interlocked.Decrement(ref _poolCount);
        }
    }
}

/// <summary>
/// RAII-style pooled object rental.
/// </summary>
public readonly struct PooledObject<T> : IDisposable where T : class, new()
{
    private readonly T _obj;
    
    /// <summary>
    /// Gets the pooled object.
    /// </summary>
    public T Value => _obj;
    
    public PooledObject()
    {
        _obj = ObjectPool<T>.Rent();
    }
    
    public void Dispose()
    {
        ObjectPool<T>.Return(_obj);
    }
}

