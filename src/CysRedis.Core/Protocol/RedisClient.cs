using System.Net;
using System.Net.Sockets;
using CysRedis.Core.Common;
using CysRedis.Core.DataStructures;

namespace CysRedis.Core.Protocol;

/// <summary>
/// Represents a connected Redis client with optimized I/O using System.IO.Pipelines.
/// </summary>
public class RedisClient : IDisposable
{
    private static long _nextId = 0;
    
    private readonly TcpClient? _tcpClient;
    private readonly Stream? _stream;
    private readonly RespPipeReader? _pipeReader;
    private readonly RespPipeWriter? _pipeWriter;
    private bool _disposed;

    // Transaction state (synchronized via _transactionLock for thread safety in I/O multi-threading)
    private readonly object _transactionLock = new();
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
    /// Last activity time (command received or response sent).
    /// </summary>
    public DateTime LastActivityAt { get; private set; }

    /// <summary>
    /// Gets the idle time since last activity.
    /// </summary>
    public TimeSpan IdleTime => DateTime.UtcNow - LastActivityAt;

    /// <summary>
    /// Last command time (for backward compatibility).
    /// </summary>
    public DateTime LastCommandAt
    {
        get => LastActivityAt;
        set => LastActivityAt = value;
    }

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
    /// Whether the connection is TLS encrypted.
    /// </summary>
    public bool IsTls { get; }

    /// <summary>
    /// Gets the RESP pipe reader.
    /// </summary>
    public RespPipeReader? PipeReader => _pipeReader;

    /// <summary>
    /// Gets the RESP pipe writer.
    /// </summary>
    public RespPipeWriter? PipeWriter => _pipeWriter;

    /// <summary>
    /// Gets whether the client is connected.
    /// </summary>
    public bool IsConnected => _tcpClient?.Connected == true && !_disposed;

    /// <summary>
    /// Creates a new Redis client wrapper with optimized I/O.
    /// </summary>
    public RedisClient(TcpClient tcpClient, RedisServerOptions? options = null)
        : this(tcpClient, tcpClient?.GetStream()!, options, false)
    {
    }

    /// <summary>
    /// Creates a new Redis client with a custom stream (for TLS connections).
    /// </summary>
    /// <param name="tcpClient">The underlying TCP client.</param>
    /// <param name="stream">The stream to use (NetworkStream or SslStream).</param>
    /// <param name="options">Server options.</param>
    /// <param name="isTls">Whether this is a TLS connection.</param>
    public RedisClient(TcpClient tcpClient, Stream stream, RedisServerOptions? options, bool isTls)
    {
        _tcpClient = tcpClient ?? throw new ArgumentNullException(nameof(tcpClient));
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        
        // Use Pipeline-based readers/writers for better performance
        options ??= RedisServerOptions.Default;
        _pipeReader = RespPipeReader.Create(_stream, options);
        _pipeWriter = RespPipeWriter.Create(_stream, options);
        
        Id = Interlocked.Increment(ref _nextId);
        Address = tcpClient.Client.RemoteEndPoint?.ToString() ?? "unknown";
        ConnectedAt = DateTime.UtcNow;
        LastActivityAt = DateTime.UtcNow;
        DatabaseIndex = 0;
        Flags = ClientFlags.None;
        IsTls = isTls;
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
        LastActivityAt = other.LastActivityAt;
        Flags = other.Flags;
        ProtocolVersion = other.ProtocolVersion;
    }

    /// <summary>
    /// Updates the last activity timestamp.
    /// </summary>
    public void UpdateActivity()
    {
        LastActivityAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Starts a transaction (MULTI).
    /// </summary>
    public void StartTransaction()
    {
        lock (_transactionLock)
        {
            _transactionQueue = new List<string[]>();
            _transactionAborted = false;
            Flags |= ClientFlags.Multi;
        }
    }

    /// <summary>
    /// Queues a command in the transaction.
    /// </summary>
    public void QueueCommand(string[] args)
    {
        lock (_transactionLock)
        {
            _transactionQueue?.Add(args);
        }
    }

    /// <summary>
    /// Gets queued commands.
    /// </summary>
    public List<string[]> GetQueuedCommands()
    {
        lock (_transactionLock)
        {
            return _transactionQueue != null ? new List<string[]>(_transactionQueue) : new List<string[]>();
        }
    }

    /// <summary>
    /// Discards the transaction.
    /// </summary>
    public void DiscardTransaction()
    {
        lock (_transactionLock)
        {
            _transactionQueue = null;
            _transactionAborted = false;
            _watchedKeys = null;
            Flags &= ~ClientFlags.Multi;
        }
    }

    /// <summary>
    /// Watches a key for modifications using version numbers.
    /// </summary>
    public void Watch(string key, RedisDatabase db)
    {
        _watchedKeys ??= new Dictionary<string, long>();
        // Store the key's current version number (monotonically increasing)
        _watchedKeys[key] = db.GetKeyVersion(key);
    }

    /// <summary>
    /// Unwatches all keys.
    /// </summary>
    public void Unwatch()
    {
        _watchedKeys = null;
    }

    /// <summary>
    /// Checks if watched keys have been modified by comparing version numbers.
    /// </summary>
    public bool CheckWatchedKeys(RedisDatabase db)
    {
        if (_watchedKeys == null) return true;

        foreach (var (key, version) in _watchedKeys)
        {
            var currentVersion = db.GetKeyVersion(key);
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
        if (_pipeReader == null) return null;
        
        var command = await _pipeReader.ReadCommandAsync(cancellationToken).ConfigureAwait(false);
        if (command != null)
            UpdateActivity();
        
        return command;
    }

    /// <summary>
    /// Writes a response to the client.
    /// </summary>
    public virtual async Task WriteResponseAsync(RespValue value, CancellationToken cancellationToken = default)
    {
        if (_pipeWriter == null) return;
        
        await _pipeWriter.WriteValueAsync(value, cancellationToken).ConfigureAwait(false);
        UpdateActivity();
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
            _pipeReader?.Dispose();
            _pipeWriter?.Dispose();
            _stream?.Close();
            _tcpClient?.Close();
        }
        catch
        {
            // Ignore errors during cleanup
        }

        GC.SuppressFinalize(this);
    }

    public override string ToString()
    {
        return $"Client#{Id} [{Address}] db={DatabaseIndex} idle={IdleTime.TotalSeconds:F0}s";
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
