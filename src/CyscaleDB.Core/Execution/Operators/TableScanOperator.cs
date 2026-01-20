using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Scans all rows from a table.
/// </summary>
public sealed class TableScanOperator : OperatorBase
{
    private readonly Table _table;
    private IEnumerator<Row>? _enumerator;

    public override TableSchema Schema => _table.Schema;

    /// <summary>
    /// The table alias (for qualified column references).
    /// </summary>
    public string? Alias { get; }

    public TableScanOperator(Table table, string? alias = null)
    {
        _table = table ?? throw new ArgumentNullException(nameof(table));
        Alias = alias;
    }

    public override void Open()
    {
        base.Open();
        _enumerator = _table.ScanTable().GetEnumerator();
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
