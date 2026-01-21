using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Operator that performs INTERSECT set operation on two result sets.
/// INTERSECT returns only rows that appear in both queries.
/// </summary>
public sealed class IntersectOperator : OperatorBase
{
    private readonly IOperator _left;
    private readonly IOperator _right;
    private readonly bool _includeAll;
    
    private List<Row>? _resultRows;
    private int _currentIndex;

    public override TableSchema Schema => _left.Schema;

    /// <summary>
    /// Creates a new INTERSECT operator.
    /// </summary>
    /// <param name="left">Left input operator</param>
    /// <param name="right">Right input operator</param>
    /// <param name="includeAll">Whether to include duplicates (INTERSECT ALL)</param>
    public IntersectOperator(IOperator left, IOperator right, bool includeAll)
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

        // Collect all rows from left
        var leftRows = new List<Row>();
        Row? row;
        while ((row = _left.Next()) != null)
        {
            leftRows.Add(row);
        }

        // Collect all rows from right
        var rightRows = new List<Row>();
        while ((row = _right.Next()) != null)
        {
            rightRows.Add(row);
        }

        _left.Close();
        _right.Close();

        if (_includeAll)
        {
            // INTERSECT ALL: include rows based on minimum count from both sets
            var rightRowCounts = new Dictionary<int, int>();
            for (int i = 0; i < rightRows.Count; i++)
            {
                rightRowCounts[i] = 1;
            }

            foreach (var leftRow in leftRows)
            {
                // Find matching row in right set
                for (int i = 0; i < rightRows.Count; i++)
                {
                    if (rightRowCounts.TryGetValue(i, out var count) && count > 0)
                    {
                        if (RowsEqual(leftRow, rightRows[i]))
                        {
                            _resultRows.Add(leftRow);
                            rightRowCounts[i]--;
                            break;
                        }
                    }
                }
            }
        }
        else
        {
            // INTERSECT: include row if it appears in both sets (no duplicates)
            var addedRows = new HashSet<int>();

            foreach (var leftRow in leftRows)
            {
                if (ContainsRow(rightRows, leftRow))
                {
                    // Only add once even if appears multiple times
                    bool isDuplicate = false;
                    foreach (var resultRow in _resultRows)
                    {
                        if (RowsEqual(resultRow, leftRow))
                        {
                            isDuplicate = true;
                            break;
                        }
                    }

                    if (!isDuplicate)
                    {
                        _resultRows.Add(leftRow);
                    }
                }
            }
        }
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
    /// Checks if a row is in the list.
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
    /// Compares two rows for equality.
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
