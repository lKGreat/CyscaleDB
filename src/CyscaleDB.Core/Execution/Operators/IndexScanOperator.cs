using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Index;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Operator that uses an index to efficiently scan rows matching a condition.
/// </summary>
public sealed class IndexScanOperator : IOperator
{
    private readonly Table _table;
    private readonly BTreeIndex _index;
    private readonly IndexScanRange _scanRange;
    private readonly string _alias;
    private readonly IExpressionEvaluator? _additionalFilter;

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
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _index = index ?? throw new ArgumentNullException(nameof(index));
        _scanRange = scanRange ?? throw new ArgumentNullException(nameof(scanRange));
        _alias = alias ?? table.Schema.TableName;
        _additionalFilter = additionalFilter;
    }

    /// <inheritdoc/>
    public void Open()
    {
        if (_isOpen)
            throw new InvalidOperationException("Operator is already open");

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

    /// <inheritdoc/>
    public void Close()
    {
        _indexEnumerator?.Dispose();
        _indexEnumerator = null;
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
