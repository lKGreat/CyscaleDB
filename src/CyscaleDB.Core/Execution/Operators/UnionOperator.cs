using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Operator that performs UNION set operation on two result sets.
/// UNION combines results from two queries and removes duplicates (unless ALL is specified).
/// </summary>
public sealed class UnionOperator : OperatorBase
{
    private readonly IOperator _left;
    private readonly IOperator _right;
    private readonly bool _includeAll;
    
    private List<Row>? _resultRows;
    private int _currentIndex;

    public override TableSchema Schema => _left.Schema;

    /// <summary>
    /// Creates a new UNION operator.
    /// </summary>
    /// <param name="left">Left input operator</param>
    /// <param name="right">Right input operator</param>
    /// <param name="includeAll">Whether to include duplicates (UNION ALL)</param>
    public UnionOperator(IOperator left, IOperator right, bool includeAll)
    {
        _left = left ?? throw new ArgumentNullException(nameof(left));
        _right = right ?? throw new ArgumentNullException(nameof(right));
        _includeAll = includeAll;
    }

    public override void Open()
    {
        base.Open();
        
        _left.Open();
        _right.Open();
        
        _resultRows = [];
        _currentIndex = 0;

        // Add all rows from left
        Row? row;
        while ((row = _left.Next()) != null)
        {
            _resultRows.Add(row);
        }

        // Add rows from right
        while ((row = _right.Next()) != null)
        {
            if (_includeAll)
            {
                // UNION ALL: include all rows
                _resultRows.Add(row);
            }
            else
            {
                // UNION: only add if not already present
                if (!ContainsRow(_resultRows, row))
                {
                    _resultRows.Add(row);
                }
            }
        }

        _left.Close();
        _right.Close();
    }

    public override Row? Next()
    {
        if (_resultRows == null)
            throw new InvalidOperationException("Operator is not open");

        if (_currentIndex < _resultRows.Count)
        {
            return _resultRows[_currentIndex++];
        }

        return null;
    }

    public override void Close()
    {
        _resultRows = null;
        _currentIndex = 0;
        base.Close();
    }

    /// <summary>
    /// Checks if a row is already in the result set (for duplicate elimination).
    /// </summary>
    private static bool ContainsRow(List<Row> rows, Row targetRow)
    {
        foreach (var row in rows)
        {
            if (RowsEqual(row, targetRow))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Compares two rows for equality (all values must match).
    /// </summary>
    private static bool RowsEqual(Row row1, Row row2)
    {
        if (row1.Values.Length != row2.Values.Length)
            return false;

        for (int i = 0; i < row1.Values.Length; i++)
        {
            if (!row1.Values[i].Equals(row2.Values[i]))
                return false;
        }

        return true;
    }
}
