using System.Buffers;

namespace CyscaleDB.Core.Common;

/// <summary>
/// Shared buffer pools for memory-efficient I/O operations.
/// Uses ArrayPool for reduced GC pressure and improved performance.
/// </summary>
public static class IoBufferPool
{
    /// <summary>
    /// The shared default buffer pool.
    /// </summary>
    public static ArrayPool<byte> Shared => ArrayPool<byte>.Shared;

    /// <summary>
    /// Pool for large buffers (up to 1MB).
    /// Used for large query results or bulk operations.
    /// </summary>
    public static readonly ArrayPool<byte> Large = ArrayPool<byte>.Create(
        maxArrayLength: 1 * 1024 * 1024,
        maxArraysPerBucket: 16);

    /// <summary>
    /// Pool for page-sized buffers.
    /// Optimized for storage engine page operations.
    /// </summary>
    public static readonly ArrayPool<byte> PagePool = ArrayPool<byte>.Create(
        maxArrayLength: Constants.PageSize * 4,  // Up to 16KB
        maxArraysPerBucket: 64);

    /// <summary>
    /// Pool for small buffers (up to 4KB).
    /// Used for protocol headers and small packets.
    /// </summary>
    public static readonly ArrayPool<byte> Small = ArrayPool<byte>.Create(
        maxArrayLength: 4 * 1024,
        maxArraysPerBucket: 128);

    /// <summary>
    /// Pool for network I/O buffers.
    /// Optimized for socket receive/send operations.
    /// </summary>
    public static readonly ArrayPool<byte> Network = ArrayPool<byte>.Create(
        maxArrayLength: 64 * 1024,
        maxArraysPerBucket: 32);

    /// <summary>
    /// Rents a buffer of at least the specified size from the appropriate pool.
    /// </summary>
    /// <param name="minimumLength">Minimum required buffer size.</param>
    /// <returns>A buffer of at least the requested size.</returns>
    public static byte[] Rent(int minimumLength)
    {
        if (minimumLength <= 4 * 1024)
            return Small.Rent(minimumLength);
        if (minimumLength <= 64 * 1024)
            return Network.Rent(minimumLength);
        if (minimumLength <= 1 * 1024 * 1024)
            return Large.Rent(minimumLength);
        return Shared.Rent(minimumLength);
    }

    /// <summary>
    /// Returns a buffer to the appropriate pool.
    /// </summary>
    /// <param name="buffer">The buffer to return.</param>
    /// <param name="clearArray">Whether to clear the buffer before returning.</param>
    public static void Return(byte[] buffer, bool clearArray = false)
    {
        if (buffer.Length <= 4 * 1024)
            Small.Return(buffer, clearArray);
        else if (buffer.Length <= 64 * 1024)
            Network.Return(buffer, clearArray);
        else if (buffer.Length <= 1 * 1024 * 1024)
            Large.Return(buffer, clearArray);
        else
            Shared.Return(buffer, clearArray);
    }

    /// <summary>
    /// Rents a page-sized buffer.
    /// </summary>
    /// <returns>A buffer of page size.</returns>
    public static byte[] RentPage() => PagePool.Rent(Constants.PageSize);

    /// <summary>
    /// Returns a page buffer to the pool.
    /// </summary>
    /// <param name="buffer">The buffer to return.</param>
    /// <param name="clearArray">Whether to clear the buffer before returning.</param>
    public static void ReturnPage(byte[] buffer, bool clearArray = false)
    {
        PagePool.Return(buffer, clearArray);
    }

    /// <summary>
    /// Creates a MemoryPool adapter for use with System.IO.Pipelines.
    /// </summary>
    /// <returns>A MemoryPool backed by ArrayPool.</returns>
    public static MemoryPool<byte> CreateMemoryPool() => new ArrayPoolMemoryPool();

    /// <summary>
    /// MemoryPool implementation backed by ArrayPool for use with Pipelines.
    /// </summary>
    private sealed class ArrayPoolMemoryPool : MemoryPool<byte>
    {
        public override int MaxBufferSize => int.MaxValue;

        public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
        {
            if (minBufferSize == -1)
                minBufferSize = 4096;
            return new ArrayPoolMemoryOwner(IoBufferPool.Rent(minBufferSize));
        }

        protected override void Dispose(bool disposing)
        {
            // Nothing to dispose - pools are static
        }
    }

    /// <summary>
    /// IMemoryOwner implementation for ArrayPool buffers.
    /// </summary>
    private sealed class ArrayPoolMemoryOwner : IMemoryOwner<byte>
    {
        private byte[]? _buffer;

        public ArrayPoolMemoryOwner(byte[] buffer)
        {
            _buffer = buffer;
        }

        public Memory<byte> Memory => _buffer ?? throw new ObjectDisposedException(nameof(ArrayPoolMemoryOwner));

        public void Dispose()
        {
            var buffer = _buffer;
            if (buffer != null)
            {
                _buffer = null;
                IoBufferPool.Return(buffer);
            }
        }
    }
}

/// <summary>
/// A disposable wrapper for rented buffers.
/// Ensures buffers are returned to the pool when disposed.
/// </summary>
public readonly struct RentedBuffer : IDisposable
{
    private readonly byte[] _buffer;
    private readonly ArrayPool<byte> _pool;
    private readonly bool _clearOnReturn;

    /// <summary>
    /// Creates a new rented buffer.
    /// </summary>
    public RentedBuffer(int minimumLength, bool clearOnReturn = false)
    {
        _buffer = IoBufferPool.Rent(minimumLength);
        _pool = ArrayPool<byte>.Shared;
        _clearOnReturn = clearOnReturn;
    }

    /// <summary>
    /// Creates a new rented buffer from a specific pool.
    /// </summary>
    public RentedBuffer(ArrayPool<byte> pool, int minimumLength, bool clearOnReturn = false)
    {
        _pool = pool;
        _buffer = pool.Rent(minimumLength);
        _clearOnReturn = clearOnReturn;
    }

    /// <summary>
    /// Gets the underlying buffer.
    /// </summary>
    public byte[] Buffer => _buffer;

    /// <summary>
    /// Gets a span over the buffer.
    /// </summary>
    public Span<byte> Span => _buffer;

    /// <summary>
    /// Gets a memory over the buffer.
    /// </summary>
    public Memory<byte> Memory => _buffer;

    /// <summary>
    /// Returns the buffer to the pool.
    /// </summary>
    public void Dispose()
    {
        if (_buffer != null)
        {
            _pool.Return(_buffer, _clearOnReturn);
        }
    }
}

/// <summary>
/// A disposable wrapper for page-sized buffers.
/// </summary>
public readonly struct RentedPageBuffer : IDisposable
{
    private readonly byte[] _buffer;

    /// <summary>
    /// Creates a new rented page buffer.
    /// </summary>
    public RentedPageBuffer()
    {
        _buffer = IoBufferPool.RentPage();
    }

    /// <summary>
    /// Gets the underlying buffer.
    /// </summary>
    public byte[] Buffer => _buffer;

    /// <summary>
    /// Gets a span over the buffer.
    /// </summary>
    public Span<byte> Span => _buffer.AsSpan(0, Constants.PageSize);

    /// <summary>
    /// Gets a memory over the buffer.
    /// </summary>
    public Memory<byte> Memory => _buffer.AsMemory(0, Constants.PageSize);

    /// <summary>
    /// Returns the buffer to the pool.
    /// </summary>
    public void Dispose()
    {
        if (_buffer != null)
        {
            IoBufferPool.ReturnPage(_buffer);
        }
    }
}
