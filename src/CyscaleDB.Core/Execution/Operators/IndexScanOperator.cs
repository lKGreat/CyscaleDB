using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Index;
using CyscaleDB.Core.Storage.Mvcc;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Operator that uses an index to efficiently scan rows matching a condition.
/// Supports MVCC for consistent reads and row-level locking.
/// </summary>
public sealed class IndexScanOperator : IOperator
{
    private readonly Table _table;
    private readonly BTreeIndex _index;
    private readonly IndexScanRange _scanRange;
    private readonly string _alias;
    private readonly IExpressionEvaluator? _additionalFilter;
    private readonly ReadView? _readView;
    private readonly VersionChainManager? _versionChainManager;
    private readonly LockingOptions? _lockingOptions;
    private readonly Logger _logger;
    private readonly List<RecordLock> _acquiredLocks = [];

    private IEnumerator<RowId>? _indexEnumerator;
    private bool _isOpen;

    /// <summary>
    /// The scan range for this index scan.
    /// </summary>
    public IndexScanRange ScanRange => _scanRange;

    /// <inheritdoc/>
    public TableSchema Schema => _table.Schema;

    /// <summary>
    /// The table alias for this scan.
    /// </summary>
    public string Alias => _alias;

    /// <summary>
    /// Gets the ReadView used for MVCC visibility checks (null if not using MVCC).
    /// </summary>
    public ReadView? ReadView => _readView;

    /// <summary>
    /// Gets the locking options for this scan.
    /// </summary>
    public LockingOptions? LockingOptions => _lockingOptions;

    /// <summary>
    /// Creates a new index scan operator.
    /// </summary>
    /// <param name="table">The table to scan.</param>
    /// <param name="index">The index to use for scanning.</param>
    /// <param name="scanRange">The range of keys to scan.</param>
    /// <param name="alias">Optional table alias.</param>
    /// <param name="additionalFilter">Optional additional filter to apply after index lookup.</param>
    public IndexScanOperator(
        Table table,
        BTreeIndex index,
        IndexScanRange scanRange,
        string? alias = null,
        IExpressionEvaluator? additionalFilter = null)
        : this(table, index, scanRange, alias, additionalFilter, null, null, null)
    {
    }

    /// <summary>
    /// Creates a new index scan operator with MVCC support.
    /// </summary>
    /// <param name="table">The table to scan.</param>
    /// <param name="index">The index to use for scanning.</param>
    /// <param name="scanRange">The range of keys to scan.</param>
    /// <param name="alias">Optional table alias.</param>
    /// <param name="additionalFilter">Optional additional filter to apply after index lookup.</param>
    /// <param name="readView">ReadView for MVCC visibility checks.</param>
    /// <param name="versionChainManager">Optional version chain manager for history traversal.</param>
    public IndexScanOperator(
        Table table,
        BTreeIndex index,
        IndexScanRange scanRange,
        string? alias,
        IExpressionEvaluator? additionalFilter,
        ReadView? readView,
        VersionChainManager? versionChainManager)
        : this(table, index, scanRange, alias, additionalFilter, readView, versionChainManager, null)
    {
    }

    /// <summary>
    /// Creates a new index scan operator with MVCC support and row locking.
    /// </summary>
    /// <param name="table">The table to scan.</param>
    /// <param name="index">The index to use for scanning.</param>
    /// <param name="scanRange">The range of keys to scan.</param>
    /// <param name="alias">Optional table alias.</param>
    /// <param name="additionalFilter">Optional additional filter to apply after index lookup.</param>
    /// <param name="readView">ReadView for MVCC visibility checks.</param>
    /// <param name="versionChainManager">Optional version chain manager for history traversal.</param>
    /// <param name="lockingOptions">Options for row-level locking.</param>
    public IndexScanOperator(
        Table table,
        BTreeIndex index,
        IndexScanRange scanRange,
        string? alias,
        IExpressionEvaluator? additionalFilter,
        ReadView? readView,
        VersionChainManager? versionChainManager,
        LockingOptions? lockingOptions)
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _index = index ?? throw new ArgumentNullException(nameof(index));
        _scanRange = scanRange ?? throw new ArgumentNullException(nameof(scanRange));
        _alias = alias ?? table.Schema.TableName;
        _additionalFilter = additionalFilter;
        _readView = readView;
        _versionChainManager = versionChainManager ?? (readView != null ? new VersionChainManager() : null);
        _lockingOptions = lockingOptions;
        _logger = LogManager.Default.GetLogger<IndexScanOperator>();
    }

    /// <inheritdoc/>
    public void Open()
    {
        if (_isOpen)
            throw new InvalidOperationException("Operator is already open");

        _acquiredLocks.Clear();

        IEnumerable<RowId> rowIds;

        if (_scanRange.IsPointLookup && _scanRange.LowKey != null)
        {
            // Exact key lookup
            rowIds = _index.Lookup(_scanRange.LowKey);
        }
        else if (_scanRange.LowKey != null || _scanRange.HighKey != null)
        {
            // Range scan with bounds
            rowIds = _index.RangeScan(_scanRange.LowKey, _scanRange.HighKey);
        }
        else
        {
            // Full index scan
            rowIds = _index.ScanAll();
        }

        _indexEnumerator = rowIds.GetEnumerator();
        _isOpen = true;
    }

    /// <inheritdoc/>
    public Row? Next()
    {
        if (!_isOpen || _indexEnumerator == null)
            throw new InvalidOperationException("Operator is not open");

        while (_indexEnumerator.MoveNext())
        {
            var rowId = _indexEnumerator.Current;

            // Fetch the actual row from the table
            var row = _table.GetRowBySlot(rowId);
            if (row == null)
                continue; // Row was deleted

            // Apply MVCC visibility check if ReadView is present
            if (_readView != null && _versionChainManager != null)
            {
                var visibleRow = _versionChainManager.FindVisibleVersion(row, _readView);
                if (visibleRow == null)
                    continue; // Row is not visible to this transaction
                row = visibleRow;
            }

            // Apply row locking if enabled
            if (_lockingOptions != null && _lockingOptions.IsLockingEnabled)
            {
                var lockResult = TryAcquireRowLock(row);
                if (!lockResult)
                {
                    if (_lockingOptions.SkipLocked)
                    {
                        _logger.Debug("Skipping locked row: {0}", row.RowId);
                        continue;
                    }
                    if (_lockingOptions.NoWait)
                    {
                        throw new LockTimeoutException(
                            $"Could not obtain lock on row {row.RowId} with NOWAIT option",
                            _lockingOptions.DatabaseName,
                            _table.Schema.TableName);
                    }
                    throw new LockTimeoutException(
                        $"Could not obtain lock on row {row.RowId}",
                        _lockingOptions.DatabaseName,
                        _table.Schema.TableName);
                }
            }

            // Apply additional filter if present
            if (_additionalFilter != null)
            {
                var filterResult = _additionalFilter.Evaluate(row);
                if (filterResult.IsNull || (filterResult.Type == DataType.Boolean && !filterResult.AsBoolean()))
                    continue;
            }

            return row;
        }

        return null;
    }

    /// <summary>
    /// Attempts to acquire a row lock.
    /// </summary>
    private bool TryAcquireRowLock(Row row)
    {
        if (_lockingOptions?.RecordLockManager == null)
            return true;

        var pkValues = GetPrimaryKeyValues(row);
        var compositeKey = IndexInfo.CreateCompositeKey(pkValues);

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
            return [DataValue.FromInt(row.RowId.PageId), DataValue.FromInt(row.RowId.SlotNumber)];
        }

        var values = new DataValue[pkColumns.Count];
        for (int i = 0; i < pkColumns.Count; i++)
        {
            values[i] = row.GetValue(pkColumns[i].Name);
        }
        return values;
    }

    /// <inheritdoc/>
    public void Close()
    {
        _indexEnumerator?.Dispose();
        _indexEnumerator = null;
        _acquiredLocks.Clear();
        _isOpen = false;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Close();
    }
}

/// <summary>
/// Represents a range of keys to scan in an index.
/// </summary>
public sealed class IndexScanRange
{
    /// <summary>
    /// The low bound of the range (null for unbounded).
    /// </summary>
    public DataValue[]? LowKey { get; }

    /// <summary>
    /// The high bound of the range (null for unbounded).
    /// </summary>
    public DataValue[]? HighKey { get; }

    /// <summary>
    /// Whether the low bound is inclusive.
    /// </summary>
    public bool LowInclusive { get; }

    /// <summary>
    /// Whether the high bound is inclusive.
    /// </summary>
    public bool HighInclusive { get; }

    /// <summary>
    /// Whether this is a point lookup (exact match).
    /// </summary>
    public bool IsPointLookup { get; }

    /// <summary>
    /// Creates a point lookup range (exact match).
    /// </summary>
    public static IndexScanRange PointLookup(DataValue[] key)
    {
        return new IndexScanRange(key, key, true, true, true);
    }

    /// <summary>
    /// Creates a range scan.
    /// </summary>
    public static IndexScanRange Range(
        DataValue[]? low,
        DataValue[]? high,
        bool lowInclusive = true,
        bool highInclusive = true)
    {
        return new IndexScanRange(low, high, lowInclusive, highInclusive, false);
    }

    /// <summary>
    /// Creates a full scan (no bounds).
    /// </summary>
    public static IndexScanRange FullScan()
    {
        return new IndexScanRange(null, null, true, true, false);
    }

    private IndexScanRange(
        DataValue[]? lowKey,
        DataValue[]? highKey,
        bool lowInclusive,
        bool highInclusive,
        bool isPointLookup)
    {
        LowKey = lowKey;
        HighKey = highKey;
        LowInclusive = lowInclusive;
        HighInclusive = highInclusive;
        IsPointLookup = isPointLookup;
    }
}
