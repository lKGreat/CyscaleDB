using System.Net;
using System.Net.Sockets;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;

namespace CysRedis.Core.Protocol;

/// <summary>
/// Represents a connected Redis client.
/// </summary>
public class RedisClient : IDisposable
{
    private static long _nextId = 0;
    
    private readonly TcpClient? _tcpClient;
    private readonly NetworkStream? _stream;
    private readonly RespReader? _reader;
    private readonly RespWriter? _writer;
    private bool _disposed;

    // Transaction state
    private List<string[]>? _transactionQueue;
    private Dictionary<string, long>? _watchedKeys;
    private bool _transactionAborted;

    /// <summary>
    /// Unique client ID.
    /// </summary>
    public long Id { get; }

    /// <summary>
    /// Client name (set by CLIENT SETNAME).
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Remote endpoint address.
    /// </summary>
    public string Address { get; }

    /// <summary>
    /// Currently selected database index.
    /// </summary>
    public int DatabaseIndex { get; set; }

    /// <summary>
    /// Connection time.
    /// </summary>
    public DateTime ConnectedAt { get; }

    /// <summary>
    /// Last command time.
    /// </summary>
    public DateTime LastCommandAt { get; set; }

    /// <summary>
    /// Whether the client is in a MULTI transaction.
    /// </summary>
    public bool InTransaction => _transactionQueue != null;

    /// <summary>
    /// Whether the transaction was aborted (due to WATCH).
    /// </summary>
    public bool TransactionAborted => _transactionAborted;

    /// <summary>
    /// Whether the client is in SUBSCRIBE mode.
    /// </summary>
    public bool InPubSubMode { get; set; }

    /// <summary>
    /// Client flags.
    /// </summary>
    public ClientFlags Flags { get; set; }

    /// <summary>
    /// RESP protocol version (2 or 3).
    /// </summary>
    public int ProtocolVersion { get; set; } = 2;

    /// <summary>
    /// Gets the RESP reader.
    /// </summary>
    public RespReader? Reader => _reader;

    /// <summary>
    /// Gets the RESP writer.
    /// </summary>
    public RespWriter? Writer => _writer;

    /// <summary>
    /// Gets whether the client is connected.
    /// </summary>
    public bool IsConnected => _tcpClient?.Connected == true && !_disposed;

    /// <summary>
    /// Creates a new Redis client wrapper.
    /// </summary>
    public RedisClient(TcpClient tcpClient)
    {
        _tcpClient = tcpClient ?? throw new ArgumentNullException(nameof(tcpClient));
        _stream = tcpClient.GetStream();
        _reader = new RespReader(_stream);
        _writer = new RespWriter(_stream);
        
        Id = Interlocked.Increment(ref _nextId);
        Address = tcpClient.Client.RemoteEndPoint?.ToString() ?? "unknown";
        ConnectedAt = DateTime.UtcNow;
        LastCommandAt = DateTime.UtcNow;
        DatabaseIndex = 0;
        Flags = ClientFlags.None;
    }

    /// <summary>
    /// Creates a client wrapper from another client (for transaction execution).
    /// </summary>
    protected RedisClient(RedisClient other)
    {
        Id = other.Id;
        Name = other.Name;
        Address = other.Address;
        DatabaseIndex = other.DatabaseIndex;
        ConnectedAt = other.ConnectedAt;
        LastCommandAt = other.LastCommandAt;
        Flags = other.Flags;
        ProtocolVersion = other.ProtocolVersion;
    }

    /// <summary>
    /// Starts a transaction (MULTI).
    /// </summary>
    public void StartTransaction()
    {
        _transactionQueue = new List<string[]>();
        _transactionAborted = false;
        Flags |= ClientFlags.Multi;
    }

    /// <summary>
    /// Queues a command in the transaction.
    /// </summary>
    public void QueueCommand(string[] args)
    {
        _transactionQueue?.Add(args);
    }

    /// <summary>
    /// Gets queued commands.
    /// </summary>
    public List<string[]> GetQueuedCommands()
    {
        return _transactionQueue ?? new List<string[]>();
    }

    /// <summary>
    /// Discards the transaction.
    /// </summary>
    public void DiscardTransaction()
    {
        _transactionQueue = null;
        _transactionAborted = false;
        _watchedKeys = null;
        Flags &= ~ClientFlags.Multi;
    }

    /// <summary>
    /// Watches a key for modifications.
    /// </summary>
    public void Watch(string key, RedisDatabase db)
    {
        _watchedKeys ??= new Dictionary<string, long>();
        // Store the key version (use hash code of value as simple version)
        var obj = db.Get(key);
        _watchedKeys[key] = obj?.GetHashCode() ?? 0;
    }

    /// <summary>
    /// Unwatches all keys.
    /// </summary>
    public void Unwatch()
    {
        _watchedKeys = null;
    }

    /// <summary>
    /// Checks if watched keys have been modified.
    /// </summary>
    public bool CheckWatchedKeys(RedisDatabase db)
    {
        if (_watchedKeys == null) return true;

        foreach (var (key, version) in _watchedKeys)
        {
            var obj = db.Get(key);
            var currentVersion = obj?.GetHashCode() ?? 0;
            if (currentVersion != version)
            {
                _transactionAborted = true;
                return false;
            }
        }
        return true;
    }

    /// <summary>
    /// Reads the next command from the client.
    /// </summary>
    public async Task<string[]?> ReadCommandAsync(CancellationToken cancellationToken = default)
    {
        if (_reader == null) return null;
        var command = await _reader.ReadCommandAsync(cancellationToken);
        if (command != null)
            LastCommandAt = DateTime.UtcNow;
        return command;
    }

    /// <summary>
    /// Writes a response to the client.
    /// </summary>
    public virtual async Task WriteResponseAsync(RespValue value, CancellationToken cancellationToken = default)
    {
        if (_writer == null) return;
        await _writer.WriteValueAsync(value, cancellationToken);
        await _writer.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Writes an OK response.
    /// </summary>
    public virtual Task WriteOkAsync(CancellationToken cancellationToken = default)
        => WriteResponseAsync(RespValue.Ok, cancellationToken);

    /// <summary>
    /// Writes an error response.
    /// </summary>
    public virtual Task WriteErrorAsync(string message, CancellationToken cancellationToken = default)
        => WriteResponseAsync(RespValue.Error(message), cancellationToken);

    /// <summary>
    /// Writes an integer response.
    /// </summary>
    public virtual Task WriteIntegerAsync(long value, CancellationToken cancellationToken = default)
        => WriteResponseAsync(new RespValue(value), cancellationToken);

    /// <summary>
    /// Writes a bulk string response.
    /// </summary>
    public virtual Task WriteBulkStringAsync(string? value, CancellationToken cancellationToken = default)
        => WriteResponseAsync(value == null ? RespValue.Null : RespValue.BulkString(value), cancellationToken);

    /// <summary>
    /// Writes a null response.
    /// </summary>
    public virtual Task WriteNullAsync(CancellationToken cancellationToken = default)
        => WriteResponseAsync(RespValue.Null, cancellationToken);

    /// <summary>
    /// Closes the client connection.
    /// </summary>
    public void Close()
    {
        Dispose();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            _stream.Close();
            _tcpClient.Close();
        }
        catch
        {
            // Ignore errors during cleanup
        }

        GC.SuppressFinalize(this);
    }

    public override string ToString()
    {
        return $"Client#{Id} [{Address}] db={DatabaseIndex}";
    }
}

/// <summary>
/// Client flags.
/// </summary>
[Flags]
public enum ClientFlags
{
    None = 0,
    Slave = 1 << 0,
    Master = 1 << 1,
    Monitor = 1 << 2,
    Multi = 1 << 3,
    Blocked = 1 << 4,
    DirtyCas = 1 << 5,
    CloseAfterReply = 1 << 6,
    Unblocked = 1 << 7,
    ReadOnly = 1 << 8,
    PubSub = 1 << 9,
    NoEvict = 1 << 10,
}
