using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Expressions;

/// <summary>
/// Interface for expression evaluators.
/// </summary>
public interface IExpressionEvaluator
{
    /// <summary>
    /// Evaluates this expression for the given row.
    /// </summary>
    DataValue Evaluate(Row row);
}

/// <summary>
/// Returns a constant value.
/// </summary>
public class ConstantEvaluator : IExpressionEvaluator
{
    private readonly DataValue _value;

    public ConstantEvaluator(DataValue value)
    {
        _value = value;
    }

    public DataValue Evaluate(Row row) => _value;
}

/// <summary>
/// Returns a column value from the row.
/// </summary>
public class ColumnEvaluator : IExpressionEvaluator
{
    private readonly int _columnIndex;

    public ColumnEvaluator(int columnIndex)
    {
        _columnIndex = columnIndex;
    }

    public DataValue Evaluate(Row row)
    {
        if (_columnIndex < 0 || _columnIndex >= row.Values.Length)
            return DataValue.Null;
        return row.Values[_columnIndex];
    }
}

/// <summary>
/// Evaluates a binary operation.
/// </summary>
public class BinaryEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _left;
    private readonly IExpressionEvaluator _right;
    private readonly BinaryOp _op;

    public BinaryEvaluator(IExpressionEvaluator left, IExpressionEvaluator right, BinaryOp op)
    {
        _left = left;
        _right = right;
        _op = op;
    }

    public DataValue Evaluate(Row row)
    {
        var leftVal = _left.Evaluate(row);
        var rightVal = _right.Evaluate(row);

        // Handle NULL
        if (leftVal.IsNull || rightVal.IsNull)
        {
            // Most operations with NULL return NULL
            if (_op != BinaryOp.IsNull && _op != BinaryOp.IsNotNull)
                return DataValue.Null;
        }

        return _op switch
        {
            // Arithmetic
            BinaryOp.Add => EvaluateAdd(leftVal, rightVal),
            BinaryOp.Subtract => EvaluateSubtract(leftVal, rightVal),
            BinaryOp.Multiply => EvaluateMultiply(leftVal, rightVal),
            BinaryOp.Divide => EvaluateDivide(leftVal, rightVal),
            BinaryOp.Modulo => EvaluateModulo(leftVal, rightVal),

            // Comparison
            BinaryOp.Equal => DataValue.FromBoolean(leftVal == rightVal),
            BinaryOp.NotEqual => DataValue.FromBoolean(leftVal != rightVal),
            BinaryOp.LessThan => DataValue.FromBoolean(leftVal < rightVal),
            BinaryOp.LessThanOrEqual => DataValue.FromBoolean(leftVal <= rightVal),
            BinaryOp.GreaterThan => DataValue.FromBoolean(leftVal > rightVal),
            BinaryOp.GreaterThanOrEqual => DataValue.FromBoolean(leftVal >= rightVal),

            // Logical
            BinaryOp.And => EvaluateAnd(leftVal, rightVal),
            BinaryOp.Or => EvaluateOr(leftVal, rightVal),

            // String
            BinaryOp.Like => EvaluateLike(leftVal, rightVal),

            _ => throw new InvalidOperationException($"Unknown binary operator: {_op}")
        };
    }

    private static DataValue EvaluateAdd(DataValue left, DataValue right)
    {
        if (left.Type.IsNumeric() && right.Type.IsNumeric())
        {
            return (left.Type, right.Type) switch
            {
                (DataType.Double, _) or (_, DataType.Double) => 
                    DataValue.FromDouble(ToDouble(left) + ToDouble(right)),
                (DataType.Float, _) or (_, DataType.Float) => 
                    DataValue.FromFloat((float)(ToDouble(left) + ToDouble(right))),
                (DataType.BigInt, _) or (_, DataType.BigInt) => 
                    DataValue.FromBigInt(ToLong(left) + ToLong(right)),
                _ => DataValue.FromInt(ToInt(left) + ToInt(right))
            };
        }
        
        if (left.Type.IsString() && right.Type.IsString())
        {
            return DataValue.FromVarChar(left.AsString() + right.AsString());
        }

        throw new InvalidOperationException($"Cannot add {left.Type} and {right.Type}");
    }

    private static DataValue EvaluateSubtract(DataValue left, DataValue right)
    {
        if (left.Type.IsNumeric() && right.Type.IsNumeric())
        {
            return (left.Type, right.Type) switch
            {
                (DataType.Double, _) or (_, DataType.Double) => 
                    DataValue.FromDouble(ToDouble(left) - ToDouble(right)),
                (DataType.Float, _) or (_, DataType.Float) => 
                    DataValue.FromFloat((float)(ToDouble(left) - ToDouble(right))),
                (DataType.BigInt, _) or (_, DataType.BigInt) => 
                    DataValue.FromBigInt(ToLong(left) - ToLong(right)),
                _ => DataValue.FromInt(ToInt(left) - ToInt(right))
            };
        }

        throw new InvalidOperationException($"Cannot subtract {left.Type} and {right.Type}");
    }

    private static DataValue EvaluateMultiply(DataValue left, DataValue right)
    {
        if (left.Type.IsNumeric() && right.Type.IsNumeric())
        {
            return (left.Type, right.Type) switch
            {
                (DataType.Double, _) or (_, DataType.Double) => 
                    DataValue.FromDouble(ToDouble(left) * ToDouble(right)),
                (DataType.Float, _) or (_, DataType.Float) => 
                    DataValue.FromFloat((float)(ToDouble(left) * ToDouble(right))),
                (DataType.BigInt, _) or (_, DataType.BigInt) => 
                    DataValue.FromBigInt(ToLong(left) * ToLong(right)),
                _ => DataValue.FromInt(ToInt(left) * ToInt(right))
            };
        }

        throw new InvalidOperationException($"Cannot multiply {left.Type} and {right.Type}");
    }

    private static DataValue EvaluateDivide(DataValue left, DataValue right)
    {
        if (left.Type.IsNumeric() && right.Type.IsNumeric())
        {
            var divisor = ToDouble(right);
            if (divisor == 0)
                return DataValue.Null; // Division by zero returns NULL

            return DataValue.FromDouble(ToDouble(left) / divisor);
        }

        throw new InvalidOperationException($"Cannot divide {left.Type} by {right.Type}");
    }

    private static DataValue EvaluateModulo(DataValue left, DataValue right)
    {
        if (left.Type.IsNumeric() && right.Type.IsNumeric())
        {
            var divisor = ToLong(right);
            if (divisor == 0)
                return DataValue.Null;

            return DataValue.FromBigInt(ToLong(left) % divisor);
        }

        throw new InvalidOperationException($"Cannot modulo {left.Type} by {right.Type}");
    }

    private static DataValue EvaluateAnd(DataValue left, DataValue right)
    {
        if (left.Type == DataType.Boolean && right.Type == DataType.Boolean)
        {
            return DataValue.FromBoolean(left.AsBoolean() && right.AsBoolean());
        }
        throw new InvalidOperationException($"Cannot AND {left.Type} and {right.Type}");
    }

    private static DataValue EvaluateOr(DataValue left, DataValue right)
    {
        if (left.Type == DataType.Boolean && right.Type == DataType.Boolean)
        {
            return DataValue.FromBoolean(left.AsBoolean() || right.AsBoolean());
        }
        throw new InvalidOperationException($"Cannot OR {left.Type} and {right.Type}");
    }

    private static DataValue EvaluateLike(DataValue left, DataValue right)
    {
        if (!left.Type.IsString() || !right.Type.IsString())
            return DataValue.FromBoolean(false);

        var str = left.AsString();
        var pattern = right.AsString();

        // Convert SQL LIKE pattern to regex
        var regex = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";

        var match = System.Text.RegularExpressions.Regex.IsMatch(
            str, regex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        return DataValue.FromBoolean(match);
    }

    private static double ToDouble(DataValue val)
    {
        return val.Type switch
        {
            DataType.TinyInt => val.AsTinyInt(),
            DataType.SmallInt => val.AsSmallInt(),
            DataType.Int => val.AsInt(),
            DataType.BigInt => val.AsBigInt(),
            DataType.Float => val.AsFloat(),
            DataType.Double => val.AsDouble(),
            DataType.Decimal => (double)val.AsDecimal(),
            _ => throw new InvalidOperationException($"Cannot convert {val.Type} to double")
        };
    }

    private static long ToLong(DataValue val)
    {
        return val.Type switch
        {
            DataType.TinyInt => val.AsTinyInt(),
            DataType.SmallInt => val.AsSmallInt(),
            DataType.Int => val.AsInt(),
            DataType.BigInt => val.AsBigInt(),
            _ => (long)ToDouble(val)
        };
    }

    private static int ToInt(DataValue val)
    {
        return val.Type switch
        {
            DataType.TinyInt => val.AsTinyInt(),
            DataType.SmallInt => val.AsSmallInt(),
            DataType.Int => val.AsInt(),
            _ => (int)ToLong(val)
        };
    }
}

/// <summary>
/// Binary operation types.
/// </summary>
public enum BinaryOp
{
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Like,
    IsNull,
    IsNotNull
}

/// <summary>
/// Evaluates a unary operation.
/// </summary>
public class UnaryEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _operand;
    private readonly UnaryOp _op;

    public UnaryEvaluator(IExpressionEvaluator operand, UnaryOp op)
    {
        _operand = operand;
        _op = op;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _operand.Evaluate(row);

        return _op switch
        {
            UnaryOp.Negate => EvaluateNegate(val),
            UnaryOp.Not => EvaluateNot(val),
            UnaryOp.IsNull => DataValue.FromBoolean(val.IsNull),
            UnaryOp.IsNotNull => DataValue.FromBoolean(!val.IsNull),
            _ => throw new InvalidOperationException($"Unknown unary operator: {_op}")
        };
    }

    private static DataValue EvaluateNegate(DataValue val)
    {
        if (val.IsNull) return DataValue.Null;

        return val.Type switch
        {
            DataType.TinyInt => DataValue.FromTinyInt((sbyte)-val.AsTinyInt()),
            DataType.SmallInt => DataValue.FromSmallInt((short)-val.AsSmallInt()),
            DataType.Int => DataValue.FromInt(-val.AsInt()),
            DataType.BigInt => DataValue.FromBigInt(-val.AsBigInt()),
            DataType.Float => DataValue.FromFloat(-val.AsFloat()),
            DataType.Double => DataValue.FromDouble(-val.AsDouble()),
            DataType.Decimal => DataValue.FromDecimal(-val.AsDecimal()),
            _ => throw new InvalidOperationException($"Cannot negate {val.Type}")
        };
    }

    private static DataValue EvaluateNot(DataValue val)
    {
        if (val.IsNull) return DataValue.Null;

        if (val.Type == DataType.Boolean)
        {
            return DataValue.FromBoolean(!val.AsBoolean());
        }

        throw new InvalidOperationException($"Cannot NOT {val.Type}");
    }
}

/// <summary>
/// Unary operation types.
/// </summary>
public enum UnaryOp
{
    Negate,
    Not,
    IsNull,
    IsNotNull
}

/// <summary>
/// Evaluates IN expression.
/// </summary>
public class InEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expression;
    private readonly List<IExpressionEvaluator> _values;
    private readonly bool _isNot;

    public InEvaluator(IExpressionEvaluator expression, List<IExpressionEvaluator> values, bool isNot = false)
    {
        _expression = expression;
        _values = values;
        _isNot = isNot;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _expression.Evaluate(row);
        if (val.IsNull)
            return DataValue.Null;

        foreach (var v in _values)
        {
            var listVal = v.Evaluate(row);
            if (!listVal.IsNull && val == listVal)
            {
                return DataValue.FromBoolean(!_isNot);
            }
        }

        return DataValue.FromBoolean(_isNot);
    }
}

/// <summary>
/// Evaluates BETWEEN expression.
/// </summary>
public class BetweenEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expression;
    private readonly IExpressionEvaluator _low;
    private readonly IExpressionEvaluator _high;
    private readonly bool _isNot;

    public BetweenEvaluator(IExpressionEvaluator expression, IExpressionEvaluator low, IExpressionEvaluator high, bool isNot = false)
    {
        _expression = expression;
        _low = low;
        _high = high;
        _isNot = isNot;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _expression.Evaluate(row);
        var lowVal = _low.Evaluate(row);
        var highVal = _high.Evaluate(row);

        if (val.IsNull || lowVal.IsNull || highVal.IsNull)
            return DataValue.Null;

        var inRange = val >= lowVal && val <= highVal;
        return DataValue.FromBoolean(_isNot ? !inRange : inRange);
    }
}
