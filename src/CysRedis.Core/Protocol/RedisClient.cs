using System.Net;
using System.Net.Sockets;
using CysRedis.Core.Common;

namespace CysRedis.Core.Protocol;

/// <summary>
/// Represents a connected Redis client.
/// </summary>
public class RedisClient : IDisposable
{
    private static long _nextId = 0;
    
    private readonly TcpClient _tcpClient;
    private readonly NetworkStream _stream;
    private readonly RespReader _reader;
    private readonly RespWriter _writer;
    private bool _disposed;

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
    public bool InTransaction { get; set; }

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
    public RespReader Reader => _reader;

    /// <summary>
    /// Gets the RESP writer.
    /// </summary>
    public RespWriter Writer => _writer;

    /// <summary>
    /// Gets whether the client is connected.
    /// </summary>
    public bool IsConnected => _tcpClient.Connected && !_disposed;

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
    /// Reads the next command from the client.
    /// </summary>
    public async Task<string[]?> ReadCommandAsync(CancellationToken cancellationToken = default)
    {
        var command = await _reader.ReadCommandAsync(cancellationToken);
        if (command != null)
            LastCommandAt = DateTime.UtcNow;
        return command;
    }

    /// <summary>
    /// Writes a response to the client.
    /// </summary>
    public async Task WriteResponseAsync(RespValue value, CancellationToken cancellationToken = default)
    {
        await _writer.WriteValueAsync(value, cancellationToken);
        await _writer.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Writes an OK response.
    /// </summary>
    public Task WriteOkAsync(CancellationToken cancellationToken = default)
        => WriteResponseAsync(RespValue.Ok, cancellationToken);

    /// <summary>
    /// Writes an error response.
    /// </summary>
    public Task WriteErrorAsync(string message, CancellationToken cancellationToken = default)
        => WriteResponseAsync(RespValue.Error(message), cancellationToken);

    /// <summary>
    /// Writes an integer response.
    /// </summary>
    public Task WriteIntegerAsync(long value, CancellationToken cancellationToken = default)
        => WriteResponseAsync(new RespValue(value), cancellationToken);

    /// <summary>
    /// Writes a bulk string response.
    /// </summary>
    public Task WriteBulkStringAsync(string? value, CancellationToken cancellationToken = default)
        => WriteResponseAsync(value == null ? RespValue.Null : RespValue.BulkString(value), cancellationToken);

    /// <summary>
    /// Writes a null response.
    /// </summary>
    public Task WriteNullAsync(CancellationToken cancellationToken = default)
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
