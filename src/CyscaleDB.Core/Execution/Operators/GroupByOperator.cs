using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Aggregate function types.
/// </summary>
public enum AggregateType
{
    Count,
    CountAll,  // COUNT(*)
    Sum,
    Avg,
    Min,
    Max,
    GroupConcat,
    BitAnd,
    BitOr,
    BitXor,
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    JsonArrayAgg,
    JsonObjectAgg
}

/// <summary>
/// Represents an aggregate function specification.
/// </summary>
public class AggregateSpec
{
    public AggregateType Type { get; }
    public IExpressionEvaluator? Expression { get; }
    public string OutputName { get; }
    public DataType OutputType { get; }
    /// <summary>
    /// Separator string for GROUP_CONCAT (default is comma).
    /// </summary>
    public string Separator { get; }
    /// <summary>
    /// Whether DISTINCT is applied to the aggregate.
    /// </summary>
    public bool IsDistinct { get; }

    public AggregateSpec(AggregateType type, IExpressionEvaluator? expression, string outputName, DataType outputType, string separator = ",", bool isDistinct = false)
    {
        Type = type;
        Expression = expression;
        OutputName = outputName;
        OutputType = outputType;
        Separator = separator;
        IsDistinct = isDistinct;
    }
}

/// <summary>
/// Represents a group-by key with its evaluator, name, and data type.
/// </summary>
public class GroupByKeySpec
{
    public IExpressionEvaluator Evaluator { get; }
    public string OutputName { get; }
    public DataType OutputType { get; }

    public GroupByKeySpec(IExpressionEvaluator evaluator, string outputName, DataType outputType)
    {
        Evaluator = evaluator;
        OutputName = outputName;
        OutputType = outputType;
    }
}

/// <summary>
/// GROUP BY operator with aggregate functions.
/// Materializes all input rows, groups them, and computes aggregates.
/// </summary>
public sealed class GroupByOperator : OperatorBase
{
    private readonly IOperator _input;
    private readonly List<GroupByKeySpec> _groupByKeys;
    private readonly List<AggregateSpec> _aggregates;
    private readonly TableSchema _outputSchema;
    private List<Row>? _resultRows;
    private int _currentIndex;

    public override TableSchema Schema => _outputSchema;

    public GroupByOperator(
        IOperator input,
        List<GroupByKeySpec> groupByKeys,
        List<AggregateSpec> aggregates,
        string databaseName,
        string tableName)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        _groupByKeys = groupByKeys;
        _aggregates = aggregates ?? throw new ArgumentNullException(nameof(aggregates));
        
        // Build output schema eagerly so it's available before Open()
        _outputSchema = BuildOutputSchema(databaseName, tableName);
    }
    
    private TableSchema BuildOutputSchema(string databaseName, string tableName)
    {
        var columns = new List<ColumnDefinition>();
        int ordinal = 0;

        // Add group by columns to schema with their original names
        foreach (var key in _groupByKeys)
        {
            var col = new ColumnDefinition(key.OutputName, key.OutputType, 255, 0, 0, true)
            {
                OrdinalPosition = ordinal++
            };
            columns.Add(col);
        }

        // Add aggregate columns to schema
        foreach (var agg in _aggregates)
        {
            var col = new ColumnDefinition(agg.OutputName, agg.OutputType, 0, 0, 0, true)
            {
                OrdinalPosition = ordinal++
            };
            columns.Add(col);
        }

        return new TableSchema(0, databaseName, tableName, columns);
    }

    public override void Open()
    {
        base.Open();
        _input.Open();

        // Materialize all input rows and group them
        var groups = new Dictionary<string, List<Row>>();
        var groupKeyValues = new Dictionary<string, DataValue[]>();

        Row? row;
        while ((row = _input.Next()) != null)
        {
            var keyValues = _groupByKeys.Select(k => k.Evaluator.Evaluate(row)).ToArray();
            var key = CreateGroupKey(keyValues);

            if (!groups.TryGetValue(key, out var group))
            {
                group = new List<Row>();
                groups[key] = group;
                groupKeyValues[key] = keyValues;
            }
            group.Add(row);
        }

        // Compute aggregates for each group
        _resultRows = new List<Row>();

        if (groups.Count == 0 && _groupByKeys.Count == 0)
        {
            // No input rows and no GROUP BY - return single row with aggregates over empty set
            var values = new DataValue[_groupByKeys.Count + _aggregates.Count];
            int idx = 0;
            foreach (var agg in _aggregates)
            {
                values[idx++] = ComputeAggregate(agg, new List<Row>());
            }
            _resultRows.Add(new Row(_outputSchema, values));
        }
        else
        {
            foreach (var kvp in groups)
            {
                var values = new DataValue[_groupByKeys.Count + _aggregates.Count];
                int idx = 0;

                // Add group key values
                var keyVals = groupKeyValues[kvp.Key];
                foreach (var keyVal in keyVals)
                {
                    values[idx++] = keyVal;
                }

                // Compute aggregates
                foreach (var agg in _aggregates)
                {
                    values[idx++] = ComputeAggregate(agg, kvp.Value);
                }

                _resultRows.Add(new Row(_outputSchema, values));
            }
        }

        _currentIndex = 0;
    }

    public override Row? Next()
    {
        if (_resultRows == null || _currentIndex >= _resultRows.Count)
            return null;

        return _resultRows[_currentIndex++];
    }

    public override void Close()
    {
        _input.Close();
        _resultRows = null;
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

    private static string CreateGroupKey(DataValue[] values)
    {
        var parts = new string[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            var val = values[i];
            parts[i] = val.IsNull ? "\0NULL\0" : val.GetRawValue()?.ToString() ?? "";
        }
        return string.Join("\x1F", parts);
    }

    private DataValue ComputeAggregate(AggregateSpec agg, List<Row> rows)
    {
        return agg.Type switch
        {
            AggregateType.CountAll => DataValue.FromBigInt(rows.Count),
            AggregateType.Count => ComputeCount(agg, rows),
            AggregateType.Sum => ComputeSum(agg, rows),
            AggregateType.Avg => ComputeAvg(agg, rows),
            AggregateType.Min => ComputeMin(agg, rows),
            AggregateType.Max => ComputeMax(agg, rows),
            AggregateType.GroupConcat => ComputeGroupConcat(agg, rows),
            AggregateType.BitAnd => ComputeBitAggregate(agg, rows, (a, b) => a & b, unchecked((long)0xFFFFFFFFFFFFFFFF)),
            AggregateType.BitOr => ComputeBitAggregate(agg, rows, (a, b) => a | b, 0),
            AggregateType.BitXor => ComputeBitAggregate(agg, rows, (a, b) => a ^ b, 0),
            AggregateType.StddevPop => ComputeStddev(agg, rows, false),
            AggregateType.StddevSamp => ComputeStddev(agg, rows, true),
            AggregateType.VarPop => ComputeVariance(agg, rows, false),
            AggregateType.VarSamp => ComputeVariance(agg, rows, true),
            AggregateType.JsonArrayAgg => ComputeJsonArrayAgg(agg, rows),
            AggregateType.JsonObjectAgg => ComputeJsonObjectAgg(agg, rows),
            _ => DataValue.Null
        };
    }

    private DataValue ComputeBitAggregate(AggregateSpec agg, List<Row> rows, Func<long, long, long> op, long identity)
    {
        if (agg.Expression == null || rows.Count == 0)
            return DataValue.FromBigInt(identity);

        long result = identity;
        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!val.IsNull)
                result = op(result, val.ToLong());
        }
        return DataValue.FromBigInt(result);
    }

    private DataValue ComputeStddev(AggregateSpec agg, List<Row> rows, bool sample)
    {
        var variance = ComputeVarianceValue(agg, rows, sample);
        if (double.IsNaN(variance)) return DataValue.Null;
        return DataValue.FromDouble(Math.Sqrt(variance));
    }

    private DataValue ComputeVariance(AggregateSpec agg, List<Row> rows, bool sample)
    {
        var variance = ComputeVarianceValue(agg, rows, sample);
        if (double.IsNaN(variance)) return DataValue.Null;
        return DataValue.FromDouble(variance);
    }

    private double ComputeVarianceValue(AggregateSpec agg, List<Row> rows, bool sample)
    {
        if (agg.Expression == null || rows.Count == 0)
            return double.NaN;

        var values = new List<double>();
        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!val.IsNull)
                values.Add(val.ToDouble());
        }

        if (values.Count == 0) return double.NaN;
        if (sample && values.Count < 2) return double.NaN;

        double mean = values.Average();
        double sumSquares = values.Sum(v => (v - mean) * (v - mean));
        return sumSquares / (sample ? values.Count - 1 : values.Count);
    }

    private DataValue ComputeJsonArrayAgg(AggregateSpec agg, List<Row> rows)
    {
        if (agg.Expression == null || rows.Count == 0)
            return DataValue.FromVarChar("[]");

        var sb = new System.Text.StringBuilder("[");
        bool first = true;
        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!first) sb.Append(',');
            first = false;
            if (val.IsNull) sb.Append("null");
            else if (val.Type is DataType.Int or DataType.BigInt or DataType.SmallInt or DataType.TinyInt)
                sb.Append(val.ToLong());
            else if (val.Type is DataType.Float or DataType.Double or DataType.Decimal)
                sb.Append(val.ToDouble().ToString(System.Globalization.CultureInfo.InvariantCulture));
            else if (val.Type == DataType.Boolean)
                sb.Append(val.AsBoolean() ? "true" : "false");
            else
                sb.Append($"\"{val.AsString().Replace("\"", "\\\"")}\"");
        }
        sb.Append(']');
        return DataValue.FromVarChar(sb.ToString());
    }

    private DataValue ComputeJsonObjectAgg(AggregateSpec agg, List<Row> rows)
    {
        // JSON_OBJECTAGG needs two expressions - key and value
        // For simplicity, we use just one expression and produce {"val": val}
        if (agg.Expression == null || rows.Count == 0)
            return DataValue.FromVarChar("{}");

        var sb = new System.Text.StringBuilder("{");
        bool first = true;
        int idx = 0;
        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!first) sb.Append(',');
            first = false;
            sb.Append($"\"{idx++}\":");
            if (val.IsNull) sb.Append("null");
            else if (val.Type is DataType.Int or DataType.BigInt or DataType.SmallInt or DataType.TinyInt)
                sb.Append(val.ToLong());
            else if (val.Type is DataType.Float or DataType.Double or DataType.Decimal)
                sb.Append(val.ToDouble().ToString(System.Globalization.CultureInfo.InvariantCulture));
            else
                sb.Append($"\"{val.AsString().Replace("\"", "\\\"")}\"");
        }
        sb.Append('}');
        return DataValue.FromVarChar(sb.ToString());
    }

    private DataValue ComputeCount(AggregateSpec agg, List<Row> rows)
    {
        if (agg.Expression == null)
            return DataValue.FromBigInt(rows.Count);

        long count = 0;
        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!val.IsNull)
                count++;
        }
        return DataValue.FromBigInt(count);
    }

    private DataValue ComputeSum(AggregateSpec agg, List<Row> rows)
    {
        if (agg.Expression == null || rows.Count == 0)
            return DataValue.Null;

        decimal sum = 0;
        bool hasValue = false;

        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!val.IsNull)
            {
                sum += ConvertToDecimal(val);
                hasValue = true;
            }
        }

        if (!hasValue)
            return DataValue.Null;

        return agg.OutputType switch
        {
            DataType.Int => DataValue.FromInt((int)sum),
            DataType.BigInt => DataValue.FromBigInt((long)sum),
            DataType.Float => DataValue.FromFloat((float)sum),
            DataType.Double => DataValue.FromDouble((double)sum),
            _ => DataValue.FromDecimal(sum)
        };
    }

    private DataValue ComputeAvg(AggregateSpec agg, List<Row> rows)
    {
        if (agg.Expression == null || rows.Count == 0)
            return DataValue.Null;

        decimal sum = 0;
        long count = 0;

        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!val.IsNull)
            {
                sum += ConvertToDecimal(val);
                count++;
            }
        }

        if (count == 0)
            return DataValue.Null;

        return DataValue.FromDouble((double)(sum / count));
    }

    private DataValue ComputeMin(AggregateSpec agg, List<Row> rows)
    {
        if (agg.Expression == null || rows.Count == 0)
            return DataValue.Null;

        DataValue? min = null;

        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!val.IsNull)
            {
                if (min == null || CompareValues(val, min.Value) < 0)
                    min = val;
            }
        }

        return min ?? DataValue.Null;
    }

    private DataValue ComputeMax(AggregateSpec agg, List<Row> rows)
    {
        if (agg.Expression == null || rows.Count == 0)
            return DataValue.Null;

        DataValue? max = null;

        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!val.IsNull)
            {
                if (max == null || CompareValues(val, max.Value) > 0)
                    max = val;
            }
        }

        return max ?? DataValue.Null;
    }

    private DataValue ComputeGroupConcat(AggregateSpec agg, List<Row> rows)
    {
        if (agg.Expression == null || rows.Count == 0)
            return DataValue.Null;

        var values = new List<string>();
        var seenValues = agg.IsDistinct ? new HashSet<string>(StringComparer.Ordinal) : null;

        foreach (var row in rows)
        {
            var val = agg.Expression.Evaluate(row);
            if (!val.IsNull)
            {
                var str = val.AsString();
                if (agg.IsDistinct)
                {
                    if (seenValues!.Add(str))
                    {
                        values.Add(str);
                    }
                }
                else
                {
                    values.Add(str);
                }
            }
        }

        if (values.Count == 0)
            return DataValue.Null;

        return DataValue.FromVarChar(string.Join(agg.Separator, values));
    }

    private static decimal ConvertToDecimal(DataValue val)
    {
        return val.Type switch
        {
            DataType.Int => val.AsInt(),
            DataType.BigInt => val.AsBigInt(),
            DataType.SmallInt => val.AsSmallInt(),
            DataType.TinyInt => val.AsTinyInt(),
            DataType.Float => (decimal)val.AsFloat(),
            DataType.Double => (decimal)val.AsDouble(),
            DataType.Decimal => val.AsDecimal(),
            _ => 0
        };
    }

    private static int CompareValues(DataValue x, DataValue y)
    {
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
            _ => 0
        };
    }
}
