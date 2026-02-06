using CyscaleDB.Core.Execution;

namespace CyscaleDB.Core.Protocol.Tds;

/// <summary>
/// Represents a TDS client session with connection state.
/// </summary>
public sealed class TdsSession : IDisposable
{
    private readonly Executor _executor;
    private bool _disposed;

    /// <summary>
    /// Unique session ID (SPID in SQL Server terminology).
    /// </summary>
    public int SessionId { get; }

    /// <summary>
    /// Gets the query executor for this session.
    /// </summary>
    public Executor Executor => _executor;

    /// <summary>
    /// Authenticated username.
    /// </summary>
    public string Username { get; set; } = "sa";

    /// <summary>
    /// Current database context.
    /// </summary>
    public string CurrentDatabase { get; set; } = "master";

    /// <summary>
    /// Client application name.
    /// </summary>
    public string ApplicationName { get; set; } = "";

    /// <summary>
    /// Client hostname.
    /// </summary>
    public string ClientHostname { get; set; } = "";

    /// <summary>
    /// Negotiated packet size.
    /// </summary>
    public int PacketSize { get; set; } = TdsConstants.DefaultPacketSize;

    /// <summary>
    /// Negotiated TDS version.
    /// </summary>
    public uint TdsVersion { get; set; } = TdsConstants.TdsVersion74;

    /// <summary>
    /// Whether the session is in a transaction.
    /// </summary>
    public bool InTransaction { get; set; }

    /// <summary>
    /// Time when the session was created.
    /// </summary>
    public DateTime ConnectedAt { get; }

    /// <summary>
    /// Remote endpoint address.
    /// </summary>
    public string RemoteAddress { get; set; } = "unknown";

    public TdsSession(int sessionId, Executor executor)
    {
        SessionId = sessionId;
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        ConnectedAt = DateTime.UtcNow;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        // Executor is lightweight and doesn't need disposal
    }
}
