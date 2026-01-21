using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Operators;

/// <summary>
/// Operator that computes window functions over a result set.
/// Window functions calculate values across a set of rows related to the current row.
/// </summary>
public sealed class WindowOperator : IOperator
{
    private readonly IOperator _input;
    private readonly List<WindowFunctionSpec> _windowFunctions;
    private readonly string _databaseName;
    private readonly string _tableName;
    private readonly Logger _logger;

    private List<Row>? _bufferedRows;
    private List<Row>? _resultRows;
    private int _currentIndex;
    private bool _isOpen;
    private TableSchema? _outputSchema;

    /// <inheritdoc/>
    public TableSchema Schema => _outputSchema ?? _input.Schema;

    /// <summary>
    /// Creates a new window operator.
    /// </summary>
    /// <param name="input">The input operator</param>
    /// <param name="windowFunctions">The window function specifications</param>
    /// <param name="databaseName">Database name for schema</param>
    /// <param name="tableName">Table name for schema</param>
    public WindowOperator(
        IOperator input,
        List<WindowFunctionSpec> windowFunctions,
        string databaseName,
        string tableName)
    {
        _input = input ?? throw new ArgumentNullException(nameof(input));
        _windowFunctions = windowFunctions ?? throw new ArgumentNullException(nameof(windowFunctions));
        _databaseName = databaseName;
        _tableName = tableName;
        _logger = LogManager.Default.GetLogger<WindowOperator>();
    }

    /// <inheritdoc/>
    public void Open()
    {
        if (_isOpen)
            throw new InvalidOperationException("Operator is already open");

        _input.Open();
        _bufferedRows = [];
        _resultRows = [];
        _currentIndex = 0;

        // Buffer all input rows (window functions need access to all rows)
        Row? row;
        while ((row = _input.Next()) != null)
        {
            _bufferedRows.Add(row);
        }

        // Build output schema (input columns + window function columns)
        BuildOutputSchema();

        // Compute window functions
        ComputeWindowFunctions();

        _isOpen = true;
    }

    private void BuildOutputSchema()
    {
        var inputSchema = _input.Schema;
        var columns = new List<ColumnDefinition>(inputSchema.Columns);

        // Add columns for each window function
        foreach (var wf in _windowFunctions)
        {
            var dataType = GetWindowFunctionResultType(wf.FunctionType);
            columns.Add(new ColumnDefinition(wf.OutputName, dataType, 255));
        }

        _outputSchema = new TableSchema(0, _databaseName, _tableName, columns);
    }

    private static DataType GetWindowFunctionResultType(WindowFunctionType funcType)
    {
        return funcType switch
        {
            WindowFunctionType.RowNumber => DataType.BigInt,
            WindowFunctionType.Rank => DataType.BigInt,
            WindowFunctionType.DenseRank => DataType.BigInt,
            WindowFunctionType.NTile => DataType.BigInt,
            WindowFunctionType.Lag => DataType.VarChar, // Depends on argument type
            WindowFunctionType.Lead => DataType.VarChar,
            WindowFunctionType.FirstValue => DataType.VarChar,
            WindowFunctionType.LastValue => DataType.VarChar,
            WindowFunctionType.NthValue => DataType.VarChar,
            WindowFunctionType.CumeDist => DataType.Double,
            WindowFunctionType.PercentRank => DataType.Double,
            _ => DataType.BigInt
        };
    }

    private void ComputeWindowFunctions()
    {
        if (_bufferedRows == null || _bufferedRows.Count == 0)
            return;

        // Process each window function
        var windowResults = new List<DataValue[]>();
        foreach (var wf in _windowFunctions)
        {
            var results = ComputeSingleWindowFunction(wf);
            windowResults.Add(results);
        }

        // Build result rows
        for (int i = 0; i < _bufferedRows.Count; i++)
        {
            var inputRow = _bufferedRows[i];
            var newValues = new DataValue[inputRow.Values.Length + _windowFunctions.Count];

            // Copy input values
            Array.Copy(inputRow.Values, newValues, inputRow.Values.Length);

            // Add window function values
            for (int j = 0; j < _windowFunctions.Count; j++)
            {
                newValues[inputRow.Values.Length + j] = windowResults[j][i];
            }

            _resultRows!.Add(new Row(_outputSchema!, newValues));
        }
    }

    private DataValue[] ComputeSingleWindowFunction(WindowFunctionSpec wf)
    {
        var results = new DataValue[_bufferedRows!.Count];

        // Get partitions
        var partitions = GetPartitions(wf.PartitionByEvaluators);

        foreach (var partition in partitions)
        {
            // Sort partition if ORDER BY is specified
            if (wf.OrderByKeys.Count > 0)
            {
                SortPartition(partition, wf.OrderByKeys);
            }

            // Compute function for each row in partition
            ComputeFunctionForPartition(wf, partition, results);
        }

        return results;
    }

    private List<List<int>> GetPartitions(List<IExpressionEvaluator> partitionBy)
    {
        var partitions = new List<List<int>>();

        if (partitionBy.Count == 0)
        {
            // No partitioning - all rows in single partition
            var singlePartition = new List<int>();
            for (int i = 0; i < _bufferedRows!.Count; i++)
            {
                singlePartition.Add(i);
            }
            partitions.Add(singlePartition);
            return partitions;
        }

        // Group rows by partition key
        var partitionMap = new Dictionary<string, List<int>>();

        for (int i = 0; i < _bufferedRows!.Count; i++)
        {
            var row = _bufferedRows[i];
            var key = ComputePartitionKey(row, partitionBy);

            if (!partitionMap.TryGetValue(key, out var partition))
            {
                partition = new List<int>();
                partitionMap[key] = partition;
            }
            partition.Add(i);
        }

        partitions.AddRange(partitionMap.Values);
        return partitions;
    }

    private string ComputePartitionKey(Row row, List<IExpressionEvaluator> partitionBy)
    {
        var parts = new List<string>();
        foreach (var eval in partitionBy)
        {
            var val = eval.Evaluate(row);
            parts.Add(val.IsNull ? "NULL" : val.ToString() ?? "");
        }
        return string.Join("|", parts);
    }

    private void SortPartition(List<int> partition, List<SortKey> orderByKeys)
    {
        partition.Sort((a, b) =>
        {
            var rowA = _bufferedRows![a];
            var rowB = _bufferedRows[b];

            foreach (var sortKey in orderByKeys)
            {
                var valA = sortKey.Expression.Evaluate(rowA);
                var valB = sortKey.Expression.Evaluate(rowB);

                var cmp = valA.CompareTo(valB);
                if (cmp != 0)
                {
                    return sortKey.Direction == SortDirection.Descending ? -cmp : cmp;
                }
            }

            return 0;
        });
    }

    private void ComputeFunctionForPartition(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        switch (wf.FunctionType)
        {
            case WindowFunctionType.RowNumber:
                ComputeRowNumber(partition, results);
                break;

            case WindowFunctionType.Rank:
                ComputeRank(wf, partition, results);
                break;

            case WindowFunctionType.DenseRank:
                ComputeDenseRank(wf, partition, results);
                break;

            case WindowFunctionType.NTile:
                ComputeNTile(wf, partition, results);
                break;

            case WindowFunctionType.Lag:
                ComputeLag(wf, partition, results);
                break;

            case WindowFunctionType.Lead:
                ComputeLead(wf, partition, results);
                break;

            case WindowFunctionType.FirstValue:
                ComputeFirstValue(wf, partition, results);
                break;

            case WindowFunctionType.LastValue:
                ComputeLastValue(wf, partition, results);
                break;

            case WindowFunctionType.Sum:
                ComputeSum(wf, partition, results);
                break;

            case WindowFunctionType.Avg:
                ComputeAvg(wf, partition, results);
                break;

            case WindowFunctionType.Min:
                ComputeMin(wf, partition, results);
                break;

            case WindowFunctionType.Max:
                ComputeMax(wf, partition, results);
                break;

            case WindowFunctionType.Count:
                ComputeCount(wf, partition, results);
                break;

            case WindowFunctionType.CumeDist:
                ComputeCumeDist(wf, partition, results);
                break;

            case WindowFunctionType.PercentRank:
                ComputePercentRank(wf, partition, results);
                break;

            case WindowFunctionType.NthValue:
                ComputeNthValue(wf, partition, results);
                break;

            default:
                _logger.Warning("Unimplemented window function: {0}", wf.FunctionType);
                foreach (var rowIndex in partition)
                {
                    results[rowIndex] = DataValue.Null;
                }
                break;
        }
    }

    private void ComputeRowNumber(List<int> partition, DataValue[] results)
    {
        for (int i = 0; i < partition.Count; i++)
        {
            results[partition[i]] = DataValue.FromBigInt(i + 1);
        }
    }

    private void ComputeRank(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        int rank = 1;
        int sameRankCount = 0;

        for (int i = 0; i < partition.Count; i++)
        {
            if (i > 0 && !AreOrderByValuesEqual(wf, partition[i], partition[i - 1]))
            {
                rank += sameRankCount;
                sameRankCount = 1;
            }
            else
            {
                sameRankCount++;
            }

            results[partition[i]] = DataValue.FromBigInt(rank);
        }
    }

    private void ComputeDenseRank(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        int rank = 1;

        for (int i = 0; i < partition.Count; i++)
        {
            if (i > 0 && !AreOrderByValuesEqual(wf, partition[i], partition[i - 1]))
            {
                rank++;
            }

            results[partition[i]] = DataValue.FromBigInt(rank);
        }
    }

    private void ComputeNTile(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        // Get N from argument
        int n = 4; // Default
        if (wf.Arguments.Count > 0)
        {
            var arg = wf.Arguments[0].Evaluate(_bufferedRows![partition[0]]);
            if (!arg.IsNull)
            {
                n = (int)arg.AsBigInt();
            }
        }

        int bucketSize = (partition.Count + n - 1) / n;

        for (int i = 0; i < partition.Count; i++)
        {
            int bucket = (i / bucketSize) + 1;
            results[partition[i]] = DataValue.FromBigInt(bucket);
        }
    }

    private void ComputeLag(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        int offset = 1; // Default lag offset
        DataValue defaultValue = DataValue.Null;

        if (wf.Arguments.Count > 1)
        {
            var offsetArg = wf.Arguments[1].Evaluate(_bufferedRows![partition[0]]);
            if (!offsetArg.IsNull)
            {
                offset = (int)offsetArg.AsBigInt();
            }
        }

        if (wf.Arguments.Count > 2)
        {
            defaultValue = wf.Arguments[2].Evaluate(_bufferedRows![partition[0]]);
        }

        for (int i = 0; i < partition.Count; i++)
        {
            int lagIndex = i - offset;
            if (lagIndex >= 0 && lagIndex < partition.Count && wf.Arguments.Count > 0)
            {
                results[partition[i]] = wf.Arguments[0].Evaluate(_bufferedRows![partition[lagIndex]]);
            }
            else
            {
                results[partition[i]] = defaultValue;
            }
        }
    }

    private void ComputeLead(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        int offset = 1; // Default lead offset
        DataValue defaultValue = DataValue.Null;

        if (wf.Arguments.Count > 1)
        {
            var offsetArg = wf.Arguments[1].Evaluate(_bufferedRows![partition[0]]);
            if (!offsetArg.IsNull)
            {
                offset = (int)offsetArg.AsBigInt();
            }
        }

        if (wf.Arguments.Count > 2)
        {
            defaultValue = wf.Arguments[2].Evaluate(_bufferedRows![partition[0]]);
        }

        for (int i = 0; i < partition.Count; i++)
        {
            int leadIndex = i + offset;
            if (leadIndex >= 0 && leadIndex < partition.Count && wf.Arguments.Count > 0)
            {
                results[partition[i]] = wf.Arguments[0].Evaluate(_bufferedRows![partition[leadIndex]]);
            }
            else
            {
                results[partition[i]] = defaultValue;
            }
        }
    }

    private void ComputeFirstValue(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0 || wf.Arguments.Count == 0)
            return;

        var firstValue = wf.Arguments[0].Evaluate(_bufferedRows![partition[0]]);

        foreach (var rowIndex in partition)
        {
            results[rowIndex] = firstValue;
        }
    }

    private void ComputeLastValue(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0 || wf.Arguments.Count == 0)
            return;

        var lastValue = wf.Arguments[0].Evaluate(_bufferedRows![partition[^1]]);

        foreach (var rowIndex in partition)
        {
            results[rowIndex] = lastValue;
        }
    }

    /// <summary>
    /// Computes NTH_VALUE - returns the value of the Nth row in the partition.
    /// </summary>
    private void ComputeNthValue(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0 || wf.Arguments.Count < 2)
        {
            foreach (var rowIndex in partition)
            {
                results[rowIndex] = DataValue.Null;
            }
            return;
        }

        // Get N from the second argument
        var nArg = wf.Arguments[1].Evaluate(_bufferedRows![partition[0]]);
        if (nArg.IsNull)
        {
            foreach (var rowIndex in partition)
            {
                results[rowIndex] = DataValue.Null;
            }
            return;
        }

        int n = (int)nArg.AsBigInt();

        // N is 1-based, check bounds
        if (n < 1 || n > partition.Count)
        {
            foreach (var rowIndex in partition)
            {
                results[rowIndex] = DataValue.Null;
            }
            return;
        }

        // Get the value from the Nth row (1-based, so use n-1 for 0-based index)
        var nthValue = wf.Arguments[0].Evaluate(_bufferedRows![partition[n - 1]]);

        foreach (var rowIndex in partition)
        {
            results[rowIndex] = nthValue;
        }
    }

    /// <summary>
    /// Computes CUME_DIST - cumulative distribution.
    /// Returns the ratio of rows less than or equal to the current row divided by total rows.
    /// Formula: (number of rows with values <= current row) / (total rows in partition)
    /// </summary>
    private void ComputeCumeDist(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0)
            return;

        int totalRows = partition.Count;

        for (int i = 0; i < partition.Count; i++)
        {
            // Count how many rows have values <= current row's value
            // For tied values, all tied rows have the same CUME_DIST
            // which equals (position of last row with same value) / total rows
            int rowsLessOrEqual = i + 1;

            // Check for ties: count all rows with the same ORDER BY values
            while (rowsLessOrEqual < partition.Count &&
                   AreOrderByValuesEqual(wf, partition[i], partition[rowsLessOrEqual]))
            {
                rowsLessOrEqual++;
            }

            double cumeDist = (double)rowsLessOrEqual / totalRows;
            
            // Assign the same value to all tied rows
            for (int j = i; j < rowsLessOrEqual && j < partition.Count; j++)
            {
                if (j == i || AreOrderByValuesEqual(wf, partition[i], partition[j]))
                {
                    results[partition[j]] = DataValue.FromDouble(cumeDist);
                }
            }

            // Skip tied rows we've already processed
            while (i + 1 < partition.Count && 
                   AreOrderByValuesEqual(wf, partition[i], partition[i + 1]))
            {
                i++;
            }
        }
    }

    /// <summary>
    /// Computes PERCENT_RANK - relative rank of the current row.
    /// Formula: (rank - 1) / (total rows in partition - 1)
    /// Returns 0 for the first row and values between 0 and 1 for others.
    /// </summary>
    private void ComputePercentRank(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0)
            return;

        // Single row partition always returns 0
        if (partition.Count == 1)
        {
            results[partition[0]] = DataValue.FromDouble(0.0);
            return;
        }

        int totalRows = partition.Count;
        int rank = 1;

        for (int i = 0; i < partition.Count; i++)
        {
            if (i > 0 && !AreOrderByValuesEqual(wf, partition[i], partition[i - 1]))
            {
                rank = i + 1;
            }

            double percentRank = (double)(rank - 1) / (totalRows - 1);
            results[partition[i]] = DataValue.FromDouble(percentRank);
        }
    }

    private bool AreOrderByValuesEqual(WindowFunctionSpec wf, int rowIndex1, int rowIndex2)
    {
        if (wf.OrderByKeys.Count == 0)
            return true;

        var row1 = _bufferedRows![rowIndex1];
        var row2 = _bufferedRows![rowIndex2];

        foreach (var sortKey in wf.OrderByKeys)
        {
            var val1 = sortKey.Expression.Evaluate(row1);
            var val2 = sortKey.Expression.Evaluate(row2);

            if (!val1.Equals(val2))
                return false;
        }

        return true;
    }

    #region Aggregate Window Functions (SUM, AVG, MIN, MAX, COUNT OVER)

    /// <summary>
    /// Computes SUM OVER for a partition.
    /// Supports frame specification (ROWS BETWEEN ... AND ...) for running totals.
    /// </summary>
    private void ComputeSum(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0 || wf.Arguments.Count == 0)
            return;

        var argEvaluator = wf.Arguments[0];

        for (int i = 0; i < partition.Count; i++)
        {
            var (startIdx, endIdx) = GetFrameBounds(wf, partition, i);
            decimal sum = 0;
            bool hasValue = false;

            for (int j = startIdx; j <= endIdx; j++)
            {
                var row = _bufferedRows![partition[j]];
                var val = argEvaluator.Evaluate(row);
                
                if (!val.IsNull)
                {
                    sum += ConvertToDecimal(val);
                    hasValue = true;
                }
            }

            results[partition[i]] = hasValue ? DataValue.FromDecimal(sum) : DataValue.Null;
        }
    }

    /// <summary>
    /// Computes AVG OVER for a partition.
    /// </summary>
    private void ComputeAvg(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0 || wf.Arguments.Count == 0)
            return;

        var argEvaluator = wf.Arguments[0];

        for (int i = 0; i < partition.Count; i++)
        {
            var (startIdx, endIdx) = GetFrameBounds(wf, partition, i);
            decimal sum = 0;
            int count = 0;

            for (int j = startIdx; j <= endIdx; j++)
            {
                var row = _bufferedRows![partition[j]];
                var val = argEvaluator.Evaluate(row);
                
                if (!val.IsNull)
                {
                    sum += ConvertToDecimal(val);
                    count++;
                }
            }

            results[partition[i]] = count > 0 
                ? DataValue.FromDouble((double)(sum / count)) 
                : DataValue.Null;
        }
    }

    /// <summary>
    /// Computes MIN OVER for a partition.
    /// </summary>
    private void ComputeMin(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0 || wf.Arguments.Count == 0)
            return;

        var argEvaluator = wf.Arguments[0];

        for (int i = 0; i < partition.Count; i++)
        {
            var (startIdx, endIdx) = GetFrameBounds(wf, partition, i);
            DataValue? minValue = null;

            for (int j = startIdx; j <= endIdx; j++)
            {
                var row = _bufferedRows![partition[j]];
                var val = argEvaluator.Evaluate(row);
                
                if (!val.IsNull)
                {
                    if (minValue == null || val.CompareTo(minValue.Value) < 0)
                    {
                        minValue = val;
                    }
                }
            }

            results[partition[i]] = minValue ?? DataValue.Null;
        }
    }

    /// <summary>
    /// Computes MAX OVER for a partition.
    /// </summary>
    private void ComputeMax(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0 || wf.Arguments.Count == 0)
            return;

        var argEvaluator = wf.Arguments[0];

        for (int i = 0; i < partition.Count; i++)
        {
            var (startIdx, endIdx) = GetFrameBounds(wf, partition, i);
            DataValue? maxValue = null;

            for (int j = startIdx; j <= endIdx; j++)
            {
                var row = _bufferedRows![partition[j]];
                var val = argEvaluator.Evaluate(row);
                
                if (!val.IsNull)
                {
                    if (maxValue == null || val.CompareTo(maxValue.Value) > 0)
                    {
                        maxValue = val;
                    }
                }
            }

            results[partition[i]] = maxValue ?? DataValue.Null;
        }
    }

    /// <summary>
    /// Computes COUNT OVER for a partition.
    /// </summary>
    private void ComputeCount(WindowFunctionSpec wf, List<int> partition, DataValue[] results)
    {
        if (partition.Count == 0)
            return;

        // COUNT(*) vs COUNT(expression)
        bool countAll = wf.Arguments.Count == 0;
        var argEvaluator = countAll ? null : wf.Arguments[0];

        for (int i = 0; i < partition.Count; i++)
        {
            var (startIdx, endIdx) = GetFrameBounds(wf, partition, i);
            long count = 0;

            for (int j = startIdx; j <= endIdx; j++)
            {
                if (countAll)
                {
                    count++;
                }
                else
                {
                    var row = _bufferedRows![partition[j]];
                    var val = argEvaluator!.Evaluate(row);
                    
                    if (!val.IsNull)
                    {
                        count++;
                    }
                }
            }

            results[partition[i]] = DataValue.FromBigInt(count);
        }
    }

    /// <summary>
    /// Gets the frame bounds for a row based on frame specification.
    /// </summary>
    private (int StartIdx, int EndIdx) GetFrameBounds(WindowFunctionSpec wf, List<int> partition, int currentRowIdx)
    {
        // Default: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running aggregate)
        // But if no ORDER BY, it's ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (full partition)
        
        if (wf.OrderByKeys.Count == 0)
        {
            // No ORDER BY: aggregate over entire partition
            return (0, partition.Count - 1);
        }

        // With ORDER BY: running aggregate from start to current row
        return (0, currentRowIdx);
    }

    /// <summary>
    /// Converts a DataValue to decimal for arithmetic operations.
    /// </summary>
    private static decimal ConvertToDecimal(DataValue val)
    {
        return val.Type switch
        {
            DataType.Int => val.AsInt(),
            DataType.BigInt => val.AsBigInt(),
            DataType.TinyInt => val.AsTinyInt(),
            DataType.SmallInt => val.AsSmallInt(),
            DataType.Float => (decimal)val.AsFloat(),
            DataType.Double => (decimal)val.AsDouble(),
            DataType.Decimal => val.AsDecimal(),
            _ => throw new InvalidOperationException($"Cannot convert {val.Type} to decimal")
        };
    }

    #endregion

    /// <inheritdoc/>
    public Row? Next()
    {
        if (!_isOpen || _resultRows == null)
            throw new InvalidOperationException("Operator is not open");

        if (_currentIndex >= _resultRows.Count)
            return null;

        return _resultRows[_currentIndex++];
    }

    /// <inheritdoc/>
    public void Close()
    {
        _input.Close();
        _bufferedRows = null;
        _resultRows = null;
        _currentIndex = 0;
        _isOpen = false;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Close();
        _input.Dispose();
    }
}

/// <summary>
/// Specification for a window function to compute.
/// </summary>
public sealed class WindowFunctionSpec
{
    /// <summary>
    /// The type of window function.
    /// </summary>
    public WindowFunctionType FunctionType { get; set; }

    /// <summary>
    /// The output column name.
    /// </summary>
    public string OutputName { get; set; } = null!;

    /// <summary>
    /// Arguments to the function (for LAG, LEAD, etc.).
    /// </summary>
    public List<IExpressionEvaluator> Arguments { get; set; } = [];

    /// <summary>
    /// Evaluators for PARTITION BY expressions.
    /// </summary>
    public List<IExpressionEvaluator> PartitionByEvaluators { get; set; } = [];

    /// <summary>
    /// ORDER BY keys within the window.
    /// </summary>
    public List<SortKey> OrderByKeys { get; set; } = [];
}

/// <summary>
/// Types of window functions.
/// </summary>
public enum WindowFunctionType
{
    RowNumber,
    Rank,
    DenseRank,
    NTile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    CumeDist,
    PercentRank,
    Sum,
    Avg,
    Min,
    Max,
    Count
}
