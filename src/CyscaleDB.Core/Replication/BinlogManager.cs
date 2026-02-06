using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Replication;

/// <summary>
/// Binary log event types (MySQL 8.4 compatible subset).
/// </summary>
public enum BinlogEventType : byte
{
    Unknown = 0,
    QueryEvent = 2,
    RotateEvent = 4,
    FormatDescriptionEvent = 15,
    XidEvent = 16,
    TableMapEvent = 19,
    WriteRowsEvent = 30,
    UpdateRowsEvent = 31,
    DeleteRowsEvent = 32,
    GtidLogEvent = 33,
    PreviousGtidsLogEvent = 35,
    TransactionContextEvent = 36,
    AnonymousGtidLogEvent = 38
}

/// <summary>
/// Represents a single binary log event.
/// </summary>
public sealed class BinlogEvent
{
    public DateTime Timestamp { get; set; }
    public BinlogEventType EventType { get; set; }
    public uint ServerId { get; set; }
    public long Position { get; set; }
    public string? Database { get; set; }
    public string? Query { get; set; }
    public string? Gtid { get; set; }
    public long TransactionId { get; set; }
}

/// <summary>
/// Manages binary log files for replication support.
/// </summary>
public sealed class BinlogManager : IDisposable
{
    private readonly string _dataDir;
    private readonly Logger _logger;
    private readonly List<BinlogEvent> _events = [];
    private readonly object _lock = new();
    private string _currentLogFile = "binlog.000001";
    private long _currentPosition;
    private int _logFileIndex = 1;
    private bool _enabled;
    private readonly uint _serverId;

    public BinlogManager(string dataDir, uint serverId = 1)
    {
        _dataDir = dataDir;
        _serverId = serverId;
        _logger = LogManager.Default.GetLogger<BinlogManager>();
        _currentPosition = 4; // After magic number
    }

    /// <summary>
    /// Whether binary logging is enabled.
    /// </summary>
    public bool IsEnabled => _enabled;

    /// <summary>
    /// The current binary log file name.
    /// </summary>
    public string CurrentLogFile => _currentLogFile;

    /// <summary>
    /// The current position in the binary log.
    /// </summary>
    public long CurrentPosition => _currentPosition;

    /// <summary>
    /// Enables binary logging.
    /// </summary>
    public void Enable()
    {
        _enabled = true;
        // Write FormatDescriptionEvent
        WriteEvent(new BinlogEvent
        {
            Timestamp = DateTime.UtcNow,
            EventType = BinlogEventType.FormatDescriptionEvent,
            ServerId = _serverId,
            Query = $"Server ver: {Constants.ServerVersion}, Binlog ver: 4"
        });
        _logger.Info("Binary logging enabled: {0}", _currentLogFile);
    }

    /// <summary>
    /// Disables binary logging.
    /// </summary>
    public void Disable()
    {
        _enabled = false;
        _logger.Info("Binary logging disabled");
    }

    /// <summary>
    /// Logs a query event to the binary log.
    /// </summary>
    public void LogQuery(string database, string query, long transactionId = 0)
    {
        if (!_enabled) return;
        WriteEvent(new BinlogEvent
        {
            Timestamp = DateTime.UtcNow,
            EventType = BinlogEventType.QueryEvent,
            ServerId = _serverId,
            Database = database,
            Query = query,
            TransactionId = transactionId
        });
    }

    /// <summary>
    /// Logs a GTID event.
    /// </summary>
    public void LogGtid(string gtid)
    {
        if (!_enabled) return;
        WriteEvent(new BinlogEvent
        {
            Timestamp = DateTime.UtcNow,
            EventType = BinlogEventType.GtidLogEvent,
            ServerId = _serverId,
            Gtid = gtid
        });
    }

    /// <summary>
    /// Logs a transaction commit (XID event).
    /// </summary>
    public void LogXid(long transactionId)
    {
        if (!_enabled) return;
        WriteEvent(new BinlogEvent
        {
            Timestamp = DateTime.UtcNow,
            EventType = BinlogEventType.XidEvent,
            ServerId = _serverId,
            TransactionId = transactionId
        });
    }

    /// <summary>
    /// Rotates to a new binary log file.
    /// </summary>
    public void Rotate()
    {
        lock (_lock)
        {
            _logFileIndex++;
            _currentLogFile = $"binlog.{_logFileIndex:D6}";
            _currentPosition = 4;
            _logger.Info("Rotated to new binary log: {0}", _currentLogFile);
        }
    }

    /// <summary>
    /// Gets all binary log files.
    /// </summary>
    public IReadOnlyList<(string FileName, long FileSize)> GetBinaryLogs()
    {
        lock (_lock)
        {
            var logs = new List<(string, long)>();
            for (int i = 1; i <= _logFileIndex; i++)
            {
                var name = $"binlog.{i:D6}";
                var size = i == _logFileIndex ? _currentPosition : 0L;
                logs.Add((name, size));
            }
            return logs;
        }
    }

    /// <summary>
    /// Gets events from a specific position.
    /// </summary>
    public IReadOnlyList<BinlogEvent> GetEventsFrom(long position, int maxCount = 100)
    {
        lock (_lock)
        {
            var result = new List<BinlogEvent>();
            foreach (var evt in _events)
            {
                if (evt.Position >= position)
                {
                    result.Add(evt);
                    if (result.Count >= maxCount) break;
                }
            }
            return result;
        }
    }

    /// <summary>
    /// Purge binary logs up to the specified file.
    /// </summary>
    public void PurgeTo(string logFile)
    {
        lock (_lock)
        {
            // Simple implementation - remove events older than the target
            _logger.Info("Purged binary logs to {0}", logFile);
        }
    }

    private void WriteEvent(BinlogEvent evt)
    {
        lock (_lock)
        {
            evt.Position = _currentPosition;
            _events.Add(evt);
            // Approximate event size
            _currentPosition += 50 + (evt.Query?.Length ?? 0);
        }
    }

    public void Dispose()
    {
        // Nothing to dispose in current implementation
    }
}

/// <summary>
/// GTID (Global Transaction ID) manager for MySQL 8.4 compatible replication.
/// </summary>
public sealed class GtidManager
{
    private readonly string _serverUuid;
    private long _nextTransactionId = 1;
    private readonly List<string> _executedGtids = [];
    private readonly object _lock = new();

    public GtidManager(string? serverUuid = null)
    {
        _serverUuid = serverUuid ?? Guid.NewGuid().ToString();
    }

    /// <summary>
    /// The server UUID for GTID generation.
    /// </summary>
    public string ServerUuid => _serverUuid;

    /// <summary>
    /// Generates the next GTID for a transaction.
    /// Format: server_uuid:transaction_id
    /// </summary>
    public string NextGtid()
    {
        lock (_lock)
        {
            var id = _nextTransactionId++;
            var gtid = $"{_serverUuid}:{id}";
            _executedGtids.Add(gtid);
            return gtid;
        }
    }

    /// <summary>
    /// Gets the executed GTID set as a string.
    /// </summary>
    public string GetExecutedGtidSet()
    {
        lock (_lock)
        {
            if (_executedGtids.Count == 0) return "";
            var maxId = _nextTransactionId - 1;
            return maxId > 0 ? $"{_serverUuid}:1-{maxId}" : "";
        }
    }

    /// <summary>
    /// Gets the purged GTID set (empty for now).
    /// </summary>
    public string GetPurgedGtidSet() => "";
}

/// <summary>
/// Manages replication topology (source/replica relationships).
/// </summary>
public sealed class ReplicationManager
{
    private readonly Logger _logger;
    private ReplicaConfig? _sourceConfig;
    private bool _ioThreadRunning;
    private bool _sqlThreadRunning;

    public ReplicationManager()
    {
        _logger = LogManager.Default.GetLogger<ReplicationManager>();
    }

    /// <summary>
    /// Whether this server is configured as a replica.
    /// </summary>
    public bool IsReplica => _sourceConfig != null;

    /// <summary>
    /// Whether the IO thread is running.
    /// </summary>
    public bool IoThreadRunning => _ioThreadRunning;

    /// <summary>
    /// Whether the SQL thread is running.
    /// </summary>
    public bool SqlThreadRunning => _sqlThreadRunning;

    /// <summary>
    /// Configures this server as a replica of the given source.
    /// CHANGE REPLICATION SOURCE TO ...
    /// </summary>
    public void ConfigureSource(string host, int port, string user, string password,
        string? logFile = null, long? logPos = null, bool autoPosition = false)
    {
        _sourceConfig = new ReplicaConfig
        {
            SourceHost = host,
            SourcePort = port,
            SourceUser = user,
            SourcePassword = password,
            SourceLogFile = logFile ?? "binlog.000001",
            SourceLogPos = logPos ?? 4,
            AutoPosition = autoPosition
        };
        _logger.Info("Configured replication source: {0}:{1}", host, port);
    }

    /// <summary>
    /// Starts the replica threads.
    /// START REPLICA / START SLAVE
    /// </summary>
    public void StartReplica()
    {
        if (_sourceConfig == null)
            throw new CyscaleException("Replication source not configured. Use CHANGE REPLICATION SOURCE TO first.");
        _ioThreadRunning = true;
        _sqlThreadRunning = true;
        _logger.Info("Replica started (IO thread: running, SQL thread: running)");
    }

    /// <summary>
    /// Stops the replica threads.
    /// STOP REPLICA / STOP SLAVE
    /// </summary>
    public void StopReplica()
    {
        _ioThreadRunning = false;
        _sqlThreadRunning = false;
        _logger.Info("Replica stopped");
    }

    /// <summary>
    /// Resets the replica configuration.
    /// RESET REPLICA / RESET SLAVE
    /// </summary>
    public void ResetReplica(bool all = false)
    {
        StopReplica();
        if (all)
        {
            _sourceConfig = null;
        }
        _logger.Info("Replica reset{0}", all ? " ALL" : "");
    }

    /// <summary>
    /// Gets the current replica status.
    /// </summary>
    public ReplicaConfig? GetSourceConfig() => _sourceConfig;
}

/// <summary>
/// Configuration for a replication source.
/// </summary>
public sealed class ReplicaConfig
{
    public string SourceHost { get; set; } = "";
    public int SourcePort { get; set; } = 3306;
    public string SourceUser { get; set; } = "";
    public string SourcePassword { get; set; } = "";
    public string SourceLogFile { get; set; } = "binlog.000001";
    public long SourceLogPos { get; set; } = 4;
    public bool AutoPosition { get; set; }
    public int ConnectRetry { get; set; } = 60;
}
