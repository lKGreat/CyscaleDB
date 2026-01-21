using System.Net;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;

namespace CyscaleDB.Core.Protocol;

/// <summary>
/// Represents a client session with comprehensive connection tracking.
/// Provides session state management, activity tracking, and capabilities negotiation.
/// </summary>
public sealed class ClientSession : IDisposable
{
    private readonly Executor _executor;
    private readonly MySqlPipeReader? _pipeReader;
    private readonly MySqlPipeWriter? _pipeWriter;
    private bool _disposed;

    /// <summary>
    /// Unique session identifier.
    /// </summary>
    public long Id { get; }

    /// <summary>
    /// Client capabilities negotiated during handshake.
    /// </summary>
    public MySqlCapabilities Capabilities { get; }

    /// <summary>
    /// Gets the query executor for this session.
    /// </summary>
    public Executor Executor => _executor;

    /// <summary>
    /// Gets the MySQL packet reader.
    /// </summary>
    public MySqlPipeReader? PipeReader => _pipeReader;

    /// <summary>
    /// Gets the MySQL packet writer.
    /// </summary>
    public MySqlPipeWriter? PipeWriter => _pipeWriter;

    /// <summary>
    /// Whether to use DEPRECATE_EOF protocol.
    /// </summary>
    public bool UseDeprecateEof => (Capabilities & MySqlCapabilities.DeprecateEof) != 0;

    /// <summary>
    /// Whether multi-statements are enabled.
    /// </summary>
    public bool MultiStatements { get; set; } = true;

    /// <summary>
    /// Whether the session is in a transaction.
    /// </summary>
    public bool InTransaction { get; set; }

    /// <summary>
    /// Whether auto-commit is enabled.
    /// </summary>
    public bool AutoCommit { get; set; } = true;

    /// <summary>
    /// Authenticated username.
    /// </summary>
    public string Username { get; set; } = "root";

    /// <summary>
    /// Client remote endpoint address.
    /// </summary>
    public string RemoteAddress { get; set; } = "unknown";

    /// <summary>
    /// Time when the session was created.
    /// </summary>
    public DateTime ConnectedAt { get; }

    /// <summary>
    /// Time of the last activity on this session.
    /// </summary>
    public DateTime LastActivityAt { get; private set; }

    /// <summary>
    /// Gets the idle time for this session.
    /// </summary>
    public TimeSpan IdleTime => DateTime.UtcNow - LastActivityAt;

    /// <summary>
    /// Gets the total session duration.
    /// </summary>
    public TimeSpan SessionDuration => DateTime.UtcNow - ConnectedAt;

    /// <summary>
    /// Number of queries executed in this session.
    /// </summary>
    public long QueryCount { get; private set; }

    /// <summary>
    /// Gets or sets the current database.
    /// </summary>
    public string CurrentDatabase
    {
        get => _executor.CurrentDatabase;
        set => _executor.CurrentDatabase = value;
    }

    private static long _sessionIdCounter;

    /// <summary>
    /// Creates a new client session.
    /// </summary>
    public ClientSession(MySqlCapabilities capabilities, Executor executor)
        : this(capabilities, executor, null, null)
    {
    }

    /// <summary>
    /// Creates a new client session with pipe reader and writer.
    /// </summary>
    public ClientSession(
        MySqlCapabilities capabilities,
        Executor executor,
        MySqlPipeReader? pipeReader,
        MySqlPipeWriter? pipeWriter)
    {
        Id = Interlocked.Increment(ref _sessionIdCounter);
        Capabilities = capabilities;
        _executor = executor;
        _pipeReader = pipeReader;
        _pipeWriter = pipeWriter;
        ConnectedAt = DateTime.UtcNow;
        LastActivityAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Updates the last activity time to now.
    /// </summary>
    public void UpdateActivity()
    {
        LastActivityAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Increments the query count and updates activity.
    /// </summary>
    public void RecordQuery()
    {
        QueryCount++;
        UpdateActivity();
    }

    /// <summary>
    /// Gets the MySQL server status flags.
    /// </summary>
    public ushort GetServerStatus()
    {
        ushort status = 0;
        if (AutoCommit)
            status |= (ushort)MySqlServerStatus.AutoCommit;
        if (InTransaction)
            status |= (ushort)MySqlServerStatus.InTransaction;
        return status;
    }

    /// <summary>
    /// Resets the session state (for COM_RESET_CONNECTION).
    /// </summary>
    public void Reset()
    {
        InTransaction = false;
        AutoCommit = true;
        MultiStatements = true;
        UpdateActivity();
    }

    /// <summary>
    /// Gets session statistics.
    /// </summary>
    public SessionStats GetStats()
    {
        return new SessionStats
        {
            SessionId = Id,
            Username = Username,
            RemoteAddress = RemoteAddress,
            Database = CurrentDatabase,
            ConnectedAt = ConnectedAt,
            LastActivityAt = LastActivityAt,
            IdleTime = IdleTime,
            SessionDuration = SessionDuration,
            QueryCount = QueryCount,
            InTransaction = InTransaction
        };
    }

    public override string ToString()
    {
        return $"Session#{Id} [{Username}@{RemoteAddress}] db={CurrentDatabase} idle={IdleTime.TotalSeconds:F0}s queries={QueryCount}";
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _pipeReader?.Dispose();
            _pipeWriter?.Dispose();
        }
    }
}

/// <summary>
/// Session statistics snapshot.
/// </summary>
public sealed class SessionStats
{
    public long SessionId { get; init; }
    public string Username { get; init; } = "";
    public string RemoteAddress { get; init; } = "";
    public string Database { get; init; } = "";
    public DateTime ConnectedAt { get; init; }
    public DateTime LastActivityAt { get; init; }
    public TimeSpan IdleTime { get; init; }
    public TimeSpan SessionDuration { get; init; }
    public long QueryCount { get; init; }
    public bool InTransaction { get; init; }
}
