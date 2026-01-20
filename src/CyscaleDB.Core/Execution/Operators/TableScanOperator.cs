using CyscaleDB.Core.Common;
using CyscaleDB.Core.Parsing.Ast;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Index;
using CyscaleDB.Core.Storage.Mvcc;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Options for row-level locking during table scans.
/// </summary>
public sealed class LockingOptions
{
    /// <summary>
    /// The locking mode (None, ForUpdate, ForShare).
    /// </summary>
    public SelectLockMode LockMode { get; }

    /// <summary>
    /// The transaction ID requesting the locks.
    /// </summary>
    public long TransactionId { get; }

    /// <summary>
    /// Whether to fail immediately if a lock cannot be acquired (NOWAIT).
    /// </summary>
    public bool NoWait { get; }

    /// <summary>
    /// Whether to skip rows that are locked by other transactions (SKIP LOCKED).
    /// </summary>
    public bool SkipLocked { get; }

    /// <summary>
    /// The database name for lock identification.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// The record lock manager to use for acquiring locks.
    /// </summary>
    public RecordLockManager? RecordLockManager { get; }

    /// <summary>
    /// Creates new locking options.
    /// </summary>
    public LockingOptions(
        SelectLockMode lockMode,
        long transactionId,
        string databaseName,
        RecordLockManager? recordLockManager = null,
        bool noWait = false,
        bool skipLocked = false)
    {
        LockMode = lockMode;
        TransactionId = transactionId;
        DatabaseName = databaseName;
        RecordLockManager = recordLockManager;
        NoWait = noWait;
        SkipLocked = skipLocked;
    }

    /// <summary>
    /// Gets default (no locking) options.
    /// </summary>
    public static LockingOptions None => new(SelectLockMode.None, 0, "");

    /// <summary>
    /// Gets the record lock type based on the lock mode.
    /// </summary>
    public RecordLockType GetRecordLockType()
    {
        return LockMode switch
        {
            SelectLockMode.ForUpdate => RecordLockType.Exclusive,
            SelectLockMode.ForShare => RecordLockType.Shared,
            _ => RecordLockType.Shared
        };
    }

    /// <summary>
    /// Whether locking is enabled.
    /// </summary>
    public bool IsLockingEnabled => LockMode != SelectLockMode.None && RecordLockManager != null;
}

/// <summary>
/// Scans all rows from a table with optional MVCC support and row locking.
/// When a ReadView is provided, only rows visible to that ReadView are returned.
/// When LockingOptions are provided, row locks are acquired as rows are read.
/// </summary>
public sealed class TableScanOperator : OperatorBase
{
    private readonly Table _table;
    private readonly ReadView? _readView;
    private readonly VersionChainManager? _versionChainManager;
    private readonly LockingOptions? _lockingOptions;
    private readonly Logger _logger;
    private IEnumerator<Row>? _enumerator;
    private readonly List<RecordLock> _acquiredLocks = [];

    public override TableSchema Schema => _table.Schema;

    /// <summary>
    /// The table alias (for qualified column references).
    /// </summary>
    public string? Alias { get; }

    /// <summary>
    /// Gets the ReadView used for MVCC visibility checks (null if not using MVCC).
    /// </summary>
    public ReadView? ReadView => _readView;

    /// <summary>
    /// Gets the locking options for this scan.
    /// </summary>
    public LockingOptions? LockingOptions => _lockingOptions;

    /// <summary>
    /// Creates a new table scan operator.
    /// </summary>
    /// <param name="table">The table to scan</param>
    /// <param name="alias">Optional table alias</param>
    public TableScanOperator(Table table, string? alias = null)
        : this(table, alias, null, null, null)
    {
    }

    /// <summary>
    /// Creates a new table scan operator with MVCC support.
    /// </summary>
    /// <param name="table">The table to scan</param>
    /// <param name="alias">Optional table alias</param>
    /// <param name="readView">ReadView for MVCC visibility checks</param>
    /// <param name="versionChainManager">Optional version chain manager for history traversal</param>
    public TableScanOperator(Table table, string? alias, ReadView? readView, VersionChainManager? versionChainManager = null)
        : this(table, alias, readView, versionChainManager, null)
    {
    }

    /// <summary>
    /// Creates a new table scan operator with MVCC support and row locking.
    /// </summary>
    /// <param name="table">The table to scan</param>
    /// <param name="alias">Optional table alias</param>
    /// <param name="readView">ReadView for MVCC visibility checks</param>
    /// <param name="versionChainManager">Optional version chain manager for history traversal</param>
    /// <param name="lockingOptions">Options for row-level locking</param>
    public TableScanOperator(
        Table table,
        string? alias,
        ReadView? readView,
        VersionChainManager? versionChainManager,
        LockingOptions? lockingOptions)
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
        Alias = alias;
        _readView = readView;
        _versionChainManager = versionChainManager ?? (readView != null ? new VersionChainManager() : null);
        _lockingOptions = lockingOptions;
        _logger = LogManager.Default.GetLogger<TableScanOperator>();
    }

    public override void Open()
    {
        base.Open();
        _acquiredLocks.Clear();
        
        // Get the base table scan enumerator
        var tableRows = _table.ScanTable();
        
        // Apply filtering and locking
        _enumerator = ProcessRows(tableRows).GetEnumerator();
    }

    /// <summary>
    /// Processes rows with MVCC filtering and optional locking.
    /// </summary>
    private IEnumerable<Row> ProcessRows(IEnumerable<Row> rows)
    {
        foreach (var row in rows)
        {
            Row? visibleRow = row;

            // Apply MVCC visibility check if enabled
            if (_readView != null && _versionChainManager != null)
            {
                visibleRow = _versionChainManager.FindVisibleVersion(row, _readView);
                if (visibleRow == null)
                    continue;
            }

            // Apply row locking if enabled
            if (_lockingOptions != null && _lockingOptions.IsLockingEnabled)
            {
                var lockResult = TryAcquireRowLock(visibleRow);
                if (!lockResult)
                {
                    // Row is locked by another transaction
                    if (_lockingOptions.SkipLocked)
                    {
                        _logger.Debug("Skipping locked row: {0}", visibleRow.RowId);
                        continue;
                    }
                    if (_lockingOptions.NoWait)
                    {
                        throw new LockTimeoutException(
                            $"Could not obtain lock on row {visibleRow.RowId} with NOWAIT option",
                            _lockingOptions.DatabaseName,
                            _table.Schema.TableName);
                    }
                    // Otherwise, the lock would wait (but for now we throw)
                    throw new LockTimeoutException(
                        $"Could not obtain lock on row {visibleRow.RowId}",
                        _lockingOptions.DatabaseName,
                        _table.Schema.TableName);
                }
            }

            yield return visibleRow;
        }
    }

    /// <summary>
    /// Attempts to acquire a row lock.
    /// </summary>
    /// <returns>True if lock was acquired, false if row is locked by another transaction</returns>
    private bool TryAcquireRowLock(Row row)
    {
        if (_lockingOptions?.RecordLockManager == null)
            return true;

        // Create a composite key from the row's primary key values
        var pkValues = GetPrimaryKeyValues(row);
        var compositeKey = IndexInfo.CreateCompositeKey(pkValues);

        // Check if lock would conflict
        if (_lockingOptions.RecordLockManager.WouldConflict(
            _lockingOptions.DatabaseName,
            _table.Schema.TableName,
            "PRIMARY",
            compositeKey,
            _lockingOptions.TransactionId,
            _lockingOptions.GetRecordLockType()))
        {
            return false;
        }

        // Acquire the lock
        var recordLock = _lockingOptions.RecordLockManager.AcquireLock(
            _lockingOptions.DatabaseName,
            _table.Schema.TableName,
            "PRIMARY",
            compositeKey,
            _lockingOptions.TransactionId,
            _lockingOptions.GetRecordLockType());

        if (recordLock != null && !recordLock.IsWaiting)
        {
            _acquiredLocks.Add(recordLock);
            _logger.Debug("Acquired {0} lock on row {1} for transaction {2}",
                _lockingOptions.LockMode, row.RowId, _lockingOptions.TransactionId);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Gets the primary key values from a row.
    /// </summary>
    private DataValue[] GetPrimaryKeyValues(Row row)
    {
        var pkColumns = _table.Schema.PrimaryKeyColumns;
        if (pkColumns.Count == 0)
        {
            // No primary key, use RowId as the key
            return [DataValue.FromInt(row.RowId.PageId), DataValue.FromInt(row.RowId.SlotNumber)];
        }

        var values = new DataValue[pkColumns.Count];
        for (int i = 0; i < pkColumns.Count; i++)
        {
            values[i] = row.GetValue(pkColumns[i].Name);
        }
        return values;
    }

    public override Row? Next()
    {
        if (_enumerator == null)
            throw new InvalidOperationException("Operator is not open");

        if (_enumerator.MoveNext())
        {
            return _enumerator.Current;
        }

        return null;
    }

    public override void Close()
    {
        _enumerator?.Dispose();
        _enumerator = null;
        // Note: Locks are released when transaction commits/rollbacks, not here
        _acquiredLocks.Clear();
        base.Close();
    }
}
