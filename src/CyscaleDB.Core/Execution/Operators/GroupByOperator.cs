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
    Max
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

    public AggregateSpec(AggregateType type, IExpressionEvaluator? expression, string outputName, DataType outputType)
    {
        Type = type;
        Expression = expression;
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
    private readonly List<IExpressionEvaluator> _groupByKeys;
    private readonly List<AggregateSpec> _aggregates;
    private readonly TableSchema _outputSchema;
    private List<Row>? _resultRows;
    private int _currentIndex;

    public override TableSchema Schema => _outputSchema;

    public GroupByOperator(
        IOperator input,
        List<IExpressionEvaluator> groupByKeys,
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

        // Add group by columns to schema
        foreach (var key in _groupByKeys)
        {
            var col = new ColumnDefinition($"group_{ordinal}", DataType.VarChar, 255, 0, 0, true)
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
            var keyValues = _groupByKeys.Select(k => k.Evaluate(row)).ToArray();
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
            _ => DataValue.Null
        };
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
