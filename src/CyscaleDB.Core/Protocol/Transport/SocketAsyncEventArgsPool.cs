using System.Buffers;
using System.Collections.Concurrent;
using System.Net.Sockets;
using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Protocol.Transport;

/// <summary>
/// High-performance pool for SocketAsyncEventArgs objects.
/// Reduces GC pressure by reusing SAEA instances and their buffers.
/// </summary>
public sealed class SocketAsyncEventArgsPool : IDisposable
{
    private readonly ConcurrentStack<SocketAsyncEventArgs> _pool;
    private readonly int _bufferSize;
    private readonly int _maxPoolSize;
    private readonly ArrayPool<byte> _bufferPool;
    private readonly Logger _logger;
    private bool _disposed;

    // Statistics
    private int _created;
    private int _pooled;
    private long _rentCount;
    private long _returnCount;
    private long _discardCount;

    /// <summary>
    /// Gets the current number of pooled objects.
    /// </summary>
    public int PooledCount => _pool.Count;

    /// <summary>
    /// Gets the total number of objects created.
    /// </summary>
    public int CreatedCount => _created;

    /// <summary>
    /// Creates a new SocketAsyncEventArgs pool.
    /// </summary>
    /// <param name="initialSize">Initial number of pre-allocated objects.</param>
    /// <param name="maxSize">Maximum pool size.</param>
    /// <param name="bufferSize">Buffer size for each SAEA.</param>
    public SocketAsyncEventArgsPool(int initialSize = 100, int maxSize = 1000, int bufferSize = 8192)
    {
        _pool = new ConcurrentStack<SocketAsyncEventArgs>();
        _bufferSize = bufferSize;
        _maxPoolSize = maxSize;
        _bufferPool = ArrayPool<byte>.Shared;
        _logger = LogManager.Default.GetLogger<SocketAsyncEventArgsPool>();

        // Pre-allocate initial objects
        for (int i = 0; i < initialSize; i++)
        {
            var args = CreateNew();
            _pool.Push(args);
            Interlocked.Increment(ref _pooled);
        }

        _logger.Debug("SocketAsyncEventArgsPool initialized with {0} objects, buffer size {1}",
            initialSize, bufferSize);
    }

    /// <summary>
    /// Rents a SocketAsyncEventArgs from the pool.
    /// </summary>
    /// <returns>A SocketAsyncEventArgs instance.</returns>
    public SocketAsyncEventArgs Rent()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(SocketAsyncEventArgsPool));
        }

        Interlocked.Increment(ref _rentCount);

        if (_pool.TryPop(out var args))
        {
            Interlocked.Decrement(ref _pooled);
            return args;
        }

        // Create new if pool is empty
        return CreateNew();
    }

    /// <summary>
    /// Returns a SocketAsyncEventArgs to the pool.
    /// </summary>
    /// <param name="args">The SAEA to return.</param>
    public void Return(SocketAsyncEventArgs args)
    {
        if (_disposed)
        {
            DisposeArgs(args);
            return;
        }

        Interlocked.Increment(ref _returnCount);

        // Reset the args for reuse
        ResetArgs(args);

        // Check if pool is full
        if (_pooled >= _maxPoolSize)
        {
            Interlocked.Increment(ref _discardCount);
            DisposeArgs(args);
            return;
        }

        _pool.Push(args);
        Interlocked.Increment(ref _pooled);
    }

    /// <summary>
    /// Creates a new SocketAsyncEventArgs with buffer.
    /// </summary>
    private SocketAsyncEventArgs CreateNew()
    {
        var args = new SocketAsyncEventArgs();
        var buffer = _bufferPool.Rent(_bufferSize);
        args.SetBuffer(buffer, 0, _bufferSize);
        args.UserToken = buffer; // Store reference for cleanup

        Interlocked.Increment(ref _created);
        return args;
    }

    /// <summary>
    /// Resets a SocketAsyncEventArgs for reuse.
    /// </summary>
    private void ResetArgs(SocketAsyncEventArgs args)
    {
        args.AcceptSocket = null;
        args.RemoteEndPoint = null;
        args.SocketError = SocketError.Success;
        args.SocketFlags = SocketFlags.None;

        // Reset buffer position
        if (args.Buffer != null)
        {
            args.SetBuffer(0, _bufferSize);
        }
    }

    /// <summary>
    /// Disposes a SocketAsyncEventArgs and its buffer.
    /// </summary>
    private void DisposeArgs(SocketAsyncEventArgs args)
    {
        // Return buffer to pool
        if (args.UserToken is byte[] buffer)
        {
            _bufferPool.Return(buffer);
        }

        args.Dispose();
    }

    /// <summary>
    /// Gets pool statistics.
    /// </summary>
    public SocketPoolStats GetStats()
    {
        return new SocketPoolStats
        {
            PooledCount = _pooled,
            CreatedCount = _created,
            MaxPoolSize = _maxPoolSize,
            BufferSize = _bufferSize,
            RentCount = _rentCount,
            ReturnCount = _returnCount,
            DiscardCount = _discardCount
        };
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        // Dispose all pooled objects
        while (_pool.TryPop(out var args))
        {
            DisposeArgs(args);
        }

        _logger.Debug("SocketAsyncEventArgsPool disposed: created={0}, discarded={1}",
            _created, _discardCount);
    }
}

/// <summary>
/// Pool statistics.
/// </summary>
public sealed class SocketPoolStats
{
    public int PooledCount { get; init; }
    public int CreatedCount { get; init; }
    public int MaxPoolSize { get; init; }
    public int BufferSize { get; init; }
    public long RentCount { get; init; }
    public long ReturnCount { get; init; }
    public long DiscardCount { get; init; }

    public override string ToString()
    {
        return $"SocketPool: pooled={PooledCount}/{MaxPoolSize}, created={CreatedCount}, discarded={DiscardCount}";
    }
}

/// <summary>
/// Extension methods for SocketAsyncEventArgsPool.
/// </summary>
public static class SocketAsyncEventArgsPoolExtensions
{
    /// <summary>
    /// Rents a SAEA and returns it when disposed.
    /// </summary>
    public static RentedSocketAsyncEventArgs RentScoped(this SocketAsyncEventArgsPool pool)
    {
        return new RentedSocketAsyncEventArgs(pool, pool.Rent());
    }
}

/// <summary>
/// RAII wrapper for rented SocketAsyncEventArgs.
/// </summary>
public readonly struct RentedSocketAsyncEventArgs : IDisposable
{
    private readonly SocketAsyncEventArgsPool _pool;
    private readonly SocketAsyncEventArgs _args;

    public RentedSocketAsyncEventArgs(SocketAsyncEventArgsPool pool, SocketAsyncEventArgs args)
    {
        _pool = pool;
        _args = args;
    }

    public SocketAsyncEventArgs Args => _args;

    public void Dispose()
    {
        _pool.Return(_args);
    }
}
