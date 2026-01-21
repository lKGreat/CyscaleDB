using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.OnlineDdl;

/// <summary>
/// Manages online DDL operations, allowing concurrent DML during schema changes.
/// 
/// Online DDL is a key feature for production databases, enabling schema modifications
/// without blocking reads and writes. This manager tracks DDL operations in progress
/// and logs concurrent DML changes to be applied after the DDL completes.
/// </summary>
public sealed class OnlineDdlManager : IDisposable
{
    private readonly Dictionary<string, DdlChangeLog> _changeLogs;
    private readonly object _lock = new();
    private readonly Logger _logger;
    private bool _disposed;

    public OnlineDdlManager()
    {
        _changeLogs = new Dictionary<string, DdlChangeLog>(StringComparer.OrdinalIgnoreCase);
        _logger = LogManager.Default.GetLogger<OnlineDdlManager>();
    }

    /// <summary>
    /// Begins an online DDL operation on a table.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="operation">Type of DDL operation</param>
    /// <returns>True if DDL can proceed, false if another DDL is in progress</returns>
    public bool BeginOnlineDdl(string databaseName, string tableName, OnlineDdlOperation operation)
    {
        var key = GetTableKey(databaseName, tableName);

        lock (_lock)
        {
            if (_changeLogs.ContainsKey(key))
            {
                _logger.Warning("Online DDL already in progress for table {0}.{1}", databaseName, tableName);
                return false;
            }

            var changeLog = new DdlChangeLog(databaseName, tableName, operation);
            _changeLogs[key] = changeLog;

            _logger.Info("Started online DDL for {0}.{1}: {2}", databaseName, tableName, operation);
            return true;
        }
    }

    /// <summary>
    /// Logs a DML change that occurred during DDL execution.
    /// These changes will be replayed after the DDL completes.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="change">DML change to log</param>
    public void LogDmlChange(string databaseName, string tableName, DmlChange change)
    {
        var key = GetTableKey(databaseName, tableName);

        lock (_lock)
        {
            if (!_changeLogs.TryGetValue(key, out var changeLog))
            {
                // No DDL in progress, no need to log
                return;
            }

            changeLog.AddChange(change);
            _logger.Trace("Logged {0} during DDL on {1}.{2}", change.Type, databaseName, tableName);
        }
    }

    /// <summary>
    /// Commits the online DDL operation, applying all logged DML changes.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <returns>List of changes that need to be applied</returns>
    public List<DmlChange> CommitOnlineDdl(string databaseName, string tableName)
    {
        var key = GetTableKey(databaseName, tableName);

        lock (_lock)
        {
            if (!_changeLogs.TryGetValue(key, out var changeLog))
            {
                throw new InvalidOperationException($"No online DDL in progress for {databaseName}.{tableName}");
            }

            var changes = changeLog.GetChanges();
            _changeLogs.Remove(key);

            _logger.Info("Committed online DDL for {0}.{1}, applying {2} logged changes",
                databaseName, tableName, changes.Count);

            return changes;
        }
    }

    /// <summary>
    /// Rolls back the online DDL operation, discarding all logged changes.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    public void RollbackOnlineDdl(string databaseName, string tableName)
    {
        var key = GetTableKey(databaseName, tableName);

        lock (_lock)
        {
            if (_changeLogs.Remove(key))
            {
                _logger.Info("Rolled back online DDL for {0}.{1}", databaseName, tableName);
            }
        }
    }

    /// <summary>
    /// Checks if an online DDL operation is in progress for a table.
    /// </summary>
    public bool IsOnlineDdlInProgress(string databaseName, string tableName)
    {
        var key = GetTableKey(databaseName, tableName);
        lock (_lock)
        {
            return _changeLogs.ContainsKey(key);
        }
    }

    /// <summary>
    /// Gets information about the DDL operation in progress.
    /// </summary>
    public DdlChangeLog? GetChangeLog(string databaseName, string tableName)
    {
        var key = GetTableKey(databaseName, tableName);
        lock (_lock)
        {
            return _changeLogs.GetValueOrDefault(key);
        }
    }

    private static string GetTableKey(string databaseName, string tableName)
    {
        return $"{databaseName}.{tableName}";
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        lock (_lock)
        {
            _changeLogs.Clear();
        }
    }
}

/// <summary>
/// Tracks changes made to a table during an online DDL operation.
/// </summary>
public sealed class DdlChangeLog
{
    private readonly List<DmlChange> _changes;
    private readonly object _lock = new();

    public string DatabaseName { get; }
    public string TableName { get; }
    public OnlineDdlOperation Operation { get; }
    public DateTime StartTime { get; }
    public int ChangeCount => _changes.Count;

    public DdlChangeLog(string databaseName, string tableName, OnlineDdlOperation operation)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        Operation = operation;
        StartTime = DateTime.UtcNow;
        _changes = new List<DmlChange>();
    }

    public void AddChange(DmlChange change)
    {
        lock (_lock)
        {
            _changes.Add(change);
        }
    }

    public List<DmlChange> GetChanges()
    {
        lock (_lock)
        {
            return new List<DmlChange>(_changes);
        }
    }

    public override string ToString()
    {
        return $"DDL on {DatabaseName}.{TableName} ({Operation}): {ChangeCount} changes";
    }
}

/// <summary>
/// Represents a DML change that occurred during DDL.
/// </summary>
public sealed class DmlChange
{
    public DmlChangeType Type { get; }
    public RowId RowId { get; }
    public byte[]? OldRowData { get; }
    public byte[]? NewRowData { get; }
    public DateTime Timestamp { get; }

    public DmlChange(DmlChangeType type, RowId rowId, byte[]? oldRowData, byte[]? newRowData)
    {
        Type = type;
        RowId = rowId;
        OldRowData = oldRowData;
        NewRowData = newRowData;
        Timestamp = DateTime.UtcNow;
    }

    public static DmlChange CreateInsert(RowId rowId, byte[] newRowData)
    {
        return new DmlChange(DmlChangeType.Insert, rowId, null, newRowData);
    }

    public static DmlChange CreateUpdate(RowId rowId, byte[] oldRowData, byte[] newRowData)
    {
        return new DmlChange(DmlChangeType.Update, rowId, oldRowData, newRowData);
    }

    public static DmlChange CreateDelete(RowId rowId, byte[] oldRowData)
    {
        return new DmlChange(DmlChangeType.Delete, rowId, oldRowData, null);
    }

    public override string ToString()
    {
        return $"{Type} on row {RowId}";
    }
}

/// <summary>
/// Type of DML change.
/// </summary>
public enum DmlChangeType
{
    Insert,
    Update,
    Delete
}

/// <summary>
/// Type of online DDL operation.
/// </summary>
public enum OnlineDdlOperation
{
    AddColumn,
    DropColumn,
    ModifyColumn,
    AddIndex,
    DropIndex,
    AddConstraint,
    DropConstraint
}
