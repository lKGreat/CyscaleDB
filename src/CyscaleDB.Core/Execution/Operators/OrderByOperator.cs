using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Sort direction for ORDER BY.
/// </summary>
public enum SortDirection
{
    Ascending,
    Descending
}

/// <summary>
/// Represents a sort key (column index and direction).
/// </summary>
public class SortKey
{
    public IExpressionEvaluator Expression { get; }
    public SortDirection Direction { get; }

    public SortKey(IExpressionEvaluator expression, SortDirection direction = SortDirection.Ascending)
    {
        Expression = expression;
        Direction = direction;
    }
}

/// <summary>
/// ORDER BY operator that sorts input rows.
/// Materializes all input rows before sorting.
/// </summary>
public sealed class OrderByOperator : OperatorBase
{
    private readonly IOperator _input;
    private readonly List<SortKey> _sortKeys;
    private List<Row>? _sortedRows;
    private int _currentIndex;

    public override TableSchema Schema => _input.Schema;

    public OrderByOperator(IOperator input, List<SortKey> sortKeys)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        _sortKeys = sortKeys ?? throw new ArgumentNullException(nameof(sortKeys));
    }

    public override void Open()
    {
        base.Open();
        _input.Open();

        // Materialize all input rows
        var rows = new List<Row>();
        Row? row;
        while ((row = _input.Next()) != null)
        {
            rows.Add(row);
        }

        // Sort rows
        _sortedRows = rows.OrderBy(r => r, new RowComparer(_sortKeys)).ToList();
        _currentIndex = 0;
    }

    public override Row? Next()
    {
        if (_sortedRows == null || _currentIndex >= _sortedRows.Count)
            return null;

        return _sortedRows[_currentIndex++];
    }

    public override void Close()
    {
        _input.Close();
        _sortedRows = null;
        _currentIndex = 0;
        base.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _input.Dispose();
        }
        base.Dispose(disposing);
    }

    private sealed class RowComparer : IComparer<Row>
    {
        private readonly List<SortKey> _sortKeys;

        public RowComparer(List<SortKey> sortKeys)
        {
            _sortKeys = sortKeys;
        }

        public int Compare(Row? x, Row? y)
        {
            if (x == null && y == null) return 0;
            if (x == null) return -1;
            if (y == null) return 1;

            foreach (var key in _sortKeys)
            {
                var valX = key.Expression.Evaluate(x);
                var valY = key.Expression.Evaluate(y);

                var cmp = CompareValues(valX, valY);

                if (cmp != 0)
                {
                    return key.Direction == SortDirection.Descending ? -cmp : cmp;
                }
            }

            return 0;
        }

        private static int CompareValues(DataValue x, DataValue y)
        {
            if (x.IsNull && y.IsNull) return 0;
            if (x.IsNull) return -1;
            if (y.IsNull) return 1;

            return x.Type switch
            {
                DataType.Int => x.AsInt().CompareTo(y.AsInt()),
                DataType.BigInt => x.AsBigInt().CompareTo(y.AsBigInt()),
                DataType.SmallInt => x.AsSmallInt().CompareTo(y.AsSmallInt()),
                DataType.TinyInt => x.AsTinyInt().CompareTo(y.AsTinyInt()),
                DataType.Float => x.AsFloat().CompareTo(y.AsFloat()),
                DataType.Double => x.AsDouble().CompareTo(y.AsDouble()),
                DataType.Decimal => x.AsDecimal().CompareTo(y.AsDecimal()),
                DataType.VarChar or DataType.Char or DataType.Text => 
                    string.Compare(x.AsString(), y.AsString(), StringComparison.Ordinal),
                DataType.DateTime => x.AsDateTime().CompareTo(y.AsDateTime()),
                DataType.Date => x.AsDate().CompareTo(y.AsDate()),
                DataType.Time => x.AsTime().CompareTo(y.AsTime()),
                DataType.Boolean => x.AsBoolean().CompareTo(y.AsBoolean()),
                _ => 0
            };
        }
    }
}
