using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Operator that performs EXCEPT set operation on two result sets.
/// EXCEPT returns rows from the left query that do not appear in the right query.
/// Also known as MINUS in some SQL dialects.
/// </summary>
public sealed class ExceptOperator : OperatorBase
{
    private readonly IOperator _left;
    private readonly IOperator _right;
    private readonly bool _includeAll;
    
    private List<Row>? _resultRows;
    private int _currentIndex;

    public override TableSchema Schema => _left.Schema;

    /// <summary>
    /// Creates a new EXCEPT operator.
    /// </summary>
    /// <param name="left">Left input operator</param>
    /// <param name="right">Right input operator</param>
    /// <param name="includeAll">Whether to include duplicates (EXCEPT ALL)</param>
    public ExceptOperator(IOperator left, IOperator right, bool includeAll)
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
            // EXCEPT ALL: subtract row counts
            // For each row in left, subtract one occurrence from right if found
            var rightRowRemaining = new List<Row>(rightRows);

            foreach (var leftRow in leftRows)
            {
                bool foundInRight = false;
                
                // Try to find and remove from right set
                for (int i = 0; i < rightRowRemaining.Count; i++)
                {
                    if (RowsEqual(leftRow, rightRowRemaining[i]))
                    {
                        rightRowRemaining.RemoveAt(i);
                        foundInRight = true;
                        break;
                    }
                }

                // If not found in right (or already consumed), include in result
                if (!foundInRight)
                {
                    _resultRows.Add(leftRow);
                }
            }
        }
        else
        {
            // EXCEPT: include row from left if it's not in right (no duplicates in result)
            var addedRows = new HashSet<int>();

            foreach (var leftRow in leftRows)
            {
                if (!ContainsRow(rightRows, leftRow))
                {
                    // Only add once even if appears multiple times in left
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
