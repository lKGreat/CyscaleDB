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

#region Spatial Evaluators

/// <summary>
/// Creates a POINT geometry from x and y coordinates.
/// </summary>
public class StPointEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _x;
    private readonly IExpressionEvaluator _y;

    public StPointEvaluator(IExpressionEvaluator x, IExpressionEvaluator y)
    {
        _x = x;
        _y = y;
    }

    public DataValue Evaluate(Row row)
    {
        var x = _x.Evaluate(row);
        var y = _y.Evaluate(row);
        if (x.IsNull || y.IsNull) return DataValue.Null;

        // Store as WKT format
        var wkt = $"POINT({x.AsDouble()} {y.AsDouble()})";
        return DataValue.FromGeometry(wkt);
    }
}

/// <summary>
/// Creates a geometry from WKT text.
/// </summary>
public class StGeomFromTextEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _wkt;
    private readonly IExpressionEvaluator? _srid;

    public StGeomFromTextEvaluator(IExpressionEvaluator wkt, IExpressionEvaluator? srid = null)
    {
        _wkt = wkt;
        _srid = srid;
    }

    public DataValue Evaluate(Row row)
    {
        var wkt = _wkt.Evaluate(row);
        if (wkt.IsNull) return DataValue.Null;

        // For now, just store the WKT string as geometry
        return DataValue.FromGeometry(wkt.AsString());
    }
}

/// <summary>
/// Returns the WKT representation of a geometry.
/// </summary>
public class StAsTextEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _geom;

    public StAsTextEvaluator(IExpressionEvaluator geom)
    {
        _geom = geom;
    }

    public DataValue Evaluate(Row row)
    {
        var geom = _geom.Evaluate(row);
        if (geom.IsNull) return DataValue.Null;
        return DataValue.FromVarChar(geom.AsString());
    }
}

/// <summary>
/// Calculates the distance between two geometries.
/// </summary>
public class StDistanceEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _geom1;
    private readonly IExpressionEvaluator _geom2;

    public StDistanceEvaluator(IExpressionEvaluator geom1, IExpressionEvaluator geom2)
    {
        _geom1 = geom1;
        _geom2 = geom2;
    }

    public DataValue Evaluate(Row row)
    {
        var g1 = _geom1.Evaluate(row);
        var g2 = _geom2.Evaluate(row);
        if (g1.IsNull || g2.IsNull) return DataValue.Null;

        // Simple 2D Euclidean distance for POINT geometries
        var (x1, y1) = ParsePoint(g1.AsString());
        var (x2, y2) = ParsePoint(g2.AsString());
        var distance = Math.Sqrt(Math.Pow(x2 - x1, 2) + Math.Pow(y2 - y1, 2));
        return DataValue.FromDouble(distance);
    }

    private static (double x, double y) ParsePoint(string wkt)
    {
        // Parse POINT(x y) format
        if (wkt.StartsWith("POINT(", StringComparison.OrdinalIgnoreCase))
        {
            var coords = wkt[6..^1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (coords.Length >= 2 &&
                double.TryParse(coords[0], out var x) &&
                double.TryParse(coords[1], out var y))
            {
                return (x, y);
            }
        }
        return (0, 0);
    }
}

/// <summary>
/// Calculates the spherical distance between two geometries using Haversine formula.
/// </summary>
public class StDistanceSphereEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _geom1;
    private readonly IExpressionEvaluator _geom2;
    private const double EarthRadiusMeters = 6371000;

    public StDistanceSphereEvaluator(IExpressionEvaluator geom1, IExpressionEvaluator geom2)
    {
        _geom1 = geom1;
        _geom2 = geom2;
    }

    public DataValue Evaluate(Row row)
    {
        var g1 = _geom1.Evaluate(row);
        var g2 = _geom2.Evaluate(row);
        if (g1.IsNull || g2.IsNull) return DataValue.Null;

        // Parse as POINT(lng lat)
        var (lng1, lat1) = ParsePoint(g1.AsString());
        var (lng2, lat2) = ParsePoint(g2.AsString());

        // Haversine formula
        var lat1Rad = lat1 * Math.PI / 180;
        var lat2Rad = lat2 * Math.PI / 180;
        var deltaLat = (lat2 - lat1) * Math.PI / 180;
        var deltaLng = (lng2 - lng1) * Math.PI / 180;

        var a = Math.Sin(deltaLat / 2) * Math.Sin(deltaLat / 2) +
                Math.Cos(lat1Rad) * Math.Cos(lat2Rad) *
                Math.Sin(deltaLng / 2) * Math.Sin(deltaLng / 2);
        var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        var distance = EarthRadiusMeters * c;

        return DataValue.FromDouble(distance);
    }

    private static (double x, double y) ParsePoint(string wkt)
    {
        if (wkt.StartsWith("POINT(", StringComparison.OrdinalIgnoreCase))
        {
            var coords = wkt[6..^1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (coords.Length >= 2 &&
                double.TryParse(coords[0], out var x) &&
                double.TryParse(coords[1], out var y))
            {
                return (x, y);
            }
        }
        return (0, 0);
    }
}

/// <summary>
/// Tests if geometry1 contains geometry2 (placeholder implementation).
/// </summary>
public class StContainsEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _geom1;
    private readonly IExpressionEvaluator _geom2;

    public StContainsEvaluator(IExpressionEvaluator geom1, IExpressionEvaluator geom2)
    {
        _geom1 = geom1;
        _geom2 = geom2;
    }

    public DataValue Evaluate(Row row)
    {
        var g1 = _geom1.Evaluate(row);
        var g2 = _geom2.Evaluate(row);
        if (g1.IsNull || g2.IsNull) return DataValue.Null;
        // Placeholder: always return false for non-point geometries
        return DataValue.FromBoolean(false);
    }
}

/// <summary>
/// Tests if geometry1 is within geometry2 (placeholder implementation).
/// </summary>
public class StWithinEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _geom1;
    private readonly IExpressionEvaluator _geom2;

    public StWithinEvaluator(IExpressionEvaluator geom1, IExpressionEvaluator geom2)
    {
        _geom1 = geom1;
        _geom2 = geom2;
    }

    public DataValue Evaluate(Row row)
    {
        var g1 = _geom1.Evaluate(row);
        var g2 = _geom2.Evaluate(row);
        if (g1.IsNull || g2.IsNull) return DataValue.Null;
        return DataValue.FromBoolean(false);
    }
}

/// <summary>
/// Tests if two geometries intersect (placeholder implementation).
/// </summary>
public class StIntersectsEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _geom1;
    private readonly IExpressionEvaluator _geom2;

    public StIntersectsEvaluator(IExpressionEvaluator geom1, IExpressionEvaluator geom2)
    {
        _geom1 = geom1;
        _geom2 = geom2;
    }

    public DataValue Evaluate(Row row)
    {
        var g1 = _geom1.Evaluate(row);
        var g2 = _geom2.Evaluate(row);
        if (g1.IsNull || g2.IsNull) return DataValue.Null;
        // For points, check if they are the same
        return DataValue.FromBoolean(g1.AsString() == g2.AsString());
    }
}

/// <summary>
/// Creates a buffer around a geometry (placeholder implementation).
/// </summary>
public class StBufferEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _geom;
    private readonly IExpressionEvaluator _distance;

    public StBufferEvaluator(IExpressionEvaluator geom, IExpressionEvaluator distance)
    {
        _geom = geom;
        _distance = distance;
    }

    public DataValue Evaluate(Row row)
    {
        var g = _geom.Evaluate(row);
        var d = _distance.Evaluate(row);
        if (g.IsNull || d.IsNull) return DataValue.Null;
        // Placeholder: return a simple circular buffer representation
        return DataValue.FromGeometry($"BUFFER({g.AsString()}, {d.AsDouble()})");
    }
}

/// <summary>
/// Returns the X coordinate of a POINT geometry.
/// </summary>
public class StXEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _point;

    public StXEvaluator(IExpressionEvaluator point)
    {
        _point = point;
    }

    public DataValue Evaluate(Row row)
    {
        var p = _point.Evaluate(row);
        if (p.IsNull) return DataValue.Null;

        var wkt = p.AsString();
        if (wkt.StartsWith("POINT(", StringComparison.OrdinalIgnoreCase))
        {
            var coords = wkt[6..^1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (coords.Length >= 1 && double.TryParse(coords[0], out var x))
            {
                return DataValue.FromDouble(x);
            }
        }
        return DataValue.Null;
    }
}

/// <summary>
/// Returns the Y coordinate of a POINT geometry.
/// </summary>
public class StYEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _point;

    public StYEvaluator(IExpressionEvaluator point)
    {
        _point = point;
    }

    public DataValue Evaluate(Row row)
    {
        var p = _point.Evaluate(row);
        if (p.IsNull) return DataValue.Null;

        var wkt = p.AsString();
        if (wkt.StartsWith("POINT(", StringComparison.OrdinalIgnoreCase))
        {
            var coords = wkt[6..^1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (coords.Length >= 2 && double.TryParse(coords[1], out var y))
            {
                return DataValue.FromDouble(y);
            }
        }
        return DataValue.Null;
    }
}

/// <summary>
/// Returns the SRID of a geometry (placeholder: always returns 0).
/// </summary>
public class StSridEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _geom;

    public StSridEvaluator(IExpressionEvaluator geom)
    {
        _geom = geom;
    }

    public DataValue Evaluate(Row row)
    {
        var g = _geom.Evaluate(row);
        if (g.IsNull) return DataValue.Null;
        // Placeholder: return default SRID 0
        return DataValue.FromInt(0);
    }
}

#endregion
