using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Performs a nested loop join between two operators.
/// </summary>
public sealed class NestedLoopJoinOperator : OperatorBase
{
    private readonly IOperator _left;
    private readonly IOperator _right;
    private readonly IExpressionEvaluator? _joinCondition;
    private readonly JoinOperatorType _joinType;
    private readonly TableSchema _outputSchema;

    private Row? _currentLeftRow;
    private bool _leftMatched;
    private List<Row>? _rightRows;
    private int _rightIndex;

    public override TableSchema Schema => _outputSchema;

    public NestedLoopJoinOperator(
        IOperator left, 
        IOperator right, 
        IExpressionEvaluator? joinCondition,
        JoinOperatorType joinType = JoinOperatorType.Inner)
    {
        _left = left ?? throw new ArgumentNullException(nameof(left));
        _right = right ?? throw new ArgumentNullException(nameof(right));
        _joinCondition = joinCondition;
        _joinType = joinType;

        // Build combined schema
        var columns = new List<ColumnDefinition>();
        int ordinal = 0;

        foreach (var col in left.Schema.Columns)
        {
            var newCol = new ColumnDefinition(
                $"{left.Schema.TableName}_{col.Name}",
                col.DataType,
                col.MaxLength,
                col.Precision,
                col.Scale,
                true) // All columns nullable in join result
            {
                OrdinalPosition = ordinal++
            };
            columns.Add(newCol);
        }

        foreach (var col in right.Schema.Columns)
        {
            var newCol = new ColumnDefinition(
                $"{right.Schema.TableName}_{col.Name}",
                col.DataType,
                col.MaxLength,
                col.Precision,
                col.Scale,
                true)
            {
                OrdinalPosition = ordinal++
            };
            columns.Add(newCol);
        }

        _outputSchema = new TableSchema(0, left.Schema.DatabaseName, "join_result", columns);
    }

    public override void Open()
    {
        base.Open();
        _left.Open();
        _right.Open();

        // Materialize right side for nested loop (simple implementation)
        _rightRows = [];
        Row? row;
        while ((row = _right.Next()) != null)
        {
            _rightRows.Add(row.Clone());
        }
        _right.Close();

        _currentLeftRow = null;
        _rightIndex = 0;
        _leftMatched = false;
    }

    public override Row? Next()
    {
        while (true)
        {
            // Get next left row if needed
            if (_currentLeftRow == null)
            {
                _currentLeftRow = _left.Next();
                if (_currentLeftRow == null)
                    return null;
                _rightIndex = 0;
                _leftMatched = false;
            }

            // Try to find a matching right row
            while (_rightIndex < _rightRows!.Count)
            {
                var rightRow = _rightRows[_rightIndex];
                _rightIndex++;

                var joinedRow = CombineRows(_currentLeftRow, rightRow);

                // Evaluate join condition
                if (_joinCondition != null)
                {
                    var result = _joinCondition.Evaluate(joinedRow);
                    if (result.Type != DataType.Boolean || !result.AsBoolean())
                    {
                        continue;
                    }
                }

                _leftMatched = true;

                if (_joinType == JoinOperatorType.Inner || 
                    _joinType == JoinOperatorType.Left ||
                    _joinType == JoinOperatorType.Full)
                {
                    return joinedRow;
                }
            }

            // Handle LEFT JOIN: emit left row with NULLs if no match
            if (!_leftMatched && (_joinType == JoinOperatorType.Left || _joinType == JoinOperatorType.Full))
            {
                var nullRightRow = CreateNullRow(_right.Schema);
                var joinedRow = CombineRows(_currentLeftRow, nullRightRow);
                _currentLeftRow = null;
                return joinedRow;
            }

            // Move to next left row
            _currentLeftRow = null;
        }
    }

    private Row CombineRows(Row left, Row right)
    {
        var values = new DataValue[_outputSchema.Columns.Count];
        
        // Copy left values
        for (int i = 0; i < left.Values.Length; i++)
        {
            values[i] = left.Values[i];
        }
        
        // Copy right values
        for (int i = 0; i < right.Values.Length; i++)
        {
            values[left.Values.Length + i] = right.Values[i];
        }

        return new Row(_outputSchema, values);
    }

    private static Row CreateNullRow(TableSchema schema)
    {
        var values = new DataValue[schema.Columns.Count];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = DataValue.Null;
        }
        return new Row(schema, values);
    }

    public override void Close()
    {
        _left.Close();
        _rightRows = null;
        base.Close();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _left.Dispose();
            _right.Dispose();
        }
        base.Dispose(disposing);
    }
}

/// <summary>
/// Types of join operations.
/// </summary>
public enum JoinOperatorType
{
    Inner,
    Left,
    Right,
    Full,
    Cross
}
