using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Mvcc;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Scans all rows from a table with optional MVCC support.
/// When a ReadView is provided, only rows visible to that ReadView are returned.
/// </summary>
public sealed class TableScanOperator : OperatorBase
{
    private readonly Table _table;
    private readonly ReadView? _readView;
    private readonly VersionChainManager? _versionChainManager;
    private IEnumerator<Row>? _enumerator;

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
    /// Creates a new table scan operator.
    /// </summary>
    /// <param name="table">The table to scan</param>
    /// <param name="alias">Optional table alias</param>
    public TableScanOperator(Table table, string? alias = null)
        : this(table, alias, null, null)
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
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
        Alias = alias;
        _readView = readView;
        _versionChainManager = versionChainManager ?? (readView != null ? new VersionChainManager() : null);
    }

    public override void Open()
    {
        base.Open();
        
        // Get the base table scan enumerator
        var tableRows = _table.ScanTable();
        
        // If we have a ReadView, wrap with MVCC filtering
        if (_readView != null && _versionChainManager != null)
        {
            _enumerator = FilterVisibleRows(tableRows).GetEnumerator();
        }
        else
        {
            _enumerator = tableRows.GetEnumerator();
        }
    }

    /// <summary>
    /// Filters rows to return only those visible to the ReadView.
    /// </summary>
    private IEnumerable<Row> FilterVisibleRows(IEnumerable<Row> rows)
    {
        foreach (var row in rows)
        {
            var visibleRow = _versionChainManager!.FindVisibleVersion(row, _readView!);
            if (visibleRow != null)
            {
                yield return visibleRow;
            }
        }
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
        base.Close();
    }
}
