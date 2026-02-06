using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Partition;

/// <summary>
/// Partition types supported by CyscaleDB (MySQL 8.4 compatible).
/// </summary>
public enum PartitionType
{
    None,
    Range,
    RangeColumns,
    List,
    ListColumns,
    Hash,
    LinearHash,
    Key,
    LinearKey
}

/// <summary>
/// Defines a partition for a table.
/// </summary>
public sealed class PartitionDefinition
{
    /// <summary>
    /// The name of the partition.
    /// </summary>
    public string Name { get; set; } = null!;

    /// <summary>
    /// The partition type.
    /// </summary>
    public PartitionType Type { get; set; }

    /// <summary>
    /// The expression used for partitioning (for RANGE/LIST/HASH).
    /// </summary>
    public string? Expression { get; set; }

    /// <summary>
    /// The columns used for RANGE COLUMNS / LIST COLUMNS / KEY partitioning.
    /// </summary>
    public List<string> Columns { get; set; } = [];

    /// <summary>
    /// The upper bound value for RANGE partitions (LESS THAN value).
    /// Use long.MaxValue for MAXVALUE.
    /// </summary>
    public object? LessThanValue { get; set; }

    /// <summary>
    /// Whether this is a LESS THAN MAXVALUE partition.
    /// </summary>
    public bool IsMaxValue { get; set; }

    /// <summary>
    /// The list of values for LIST partitions (IN (...)).
    /// </summary>
    public List<object> InValues { get; set; } = [];

    /// <summary>
    /// Sub-partitions.
    /// </summary>
    public List<SubPartitionDefinition> SubPartitions { get; set; } = [];

    /// <summary>
    /// The comment for this partition.
    /// </summary>
    public string? Comment { get; set; }
}

/// <summary>
/// Defines a sub-partition within a partition.
/// </summary>
public sealed class SubPartitionDefinition
{
    public string Name { get; set; } = null!;
    public string? Comment { get; set; }
}

/// <summary>
/// Schema-level partitioning configuration for a table.
/// </summary>
public sealed class PartitionSchema
{
    /// <summary>
    /// The type of partitioning used.
    /// </summary>
    public PartitionType Type { get; set; }

    /// <summary>
    /// The expression used for partitioning (e.g., column name, expression).
    /// </summary>
    public string? Expression { get; set; }

    /// <summary>
    /// The columns used for COLUMNS partitioning.
    /// </summary>
    public List<string> Columns { get; set; } = [];

    /// <summary>
    /// The number of partitions (for HASH/KEY).
    /// </summary>
    public int? NumPartitions { get; set; }

    /// <summary>
    /// The partition definitions.
    /// </summary>
    public List<PartitionDefinition> Partitions { get; set; } = [];

    /// <summary>
    /// Sub-partition type (if any).
    /// </summary>
    public PartitionType SubPartitionType { get; set; } = PartitionType.None;

    /// <summary>
    /// Sub-partition expression.
    /// </summary>
    public string? SubPartitionExpression { get; set; }

    /// <summary>
    /// Number of sub-partitions per partition.
    /// </summary>
    public int? NumSubPartitions { get; set; }
}

/// <summary>
/// Manages table partitioning operations.
/// </summary>
public sealed class PartitionManager
{
    private readonly Logger _logger;

    public PartitionManager()
    {
        _logger = LogManager.Default.GetLogger<PartitionManager>();
    }

    /// <summary>
    /// Determines which partition a row belongs to based on the partition schema.
    /// Returns the partition index (0-based).
    /// </summary>
    public int GetPartitionIndex(PartitionSchema schema, DataValue[] rowValues, List<ColumnDefinition> columns)
    {
        return schema.Type switch
        {
            PartitionType.Range or PartitionType.RangeColumns => GetRangePartitionIndex(schema, rowValues, columns),
            PartitionType.List or PartitionType.ListColumns => GetListPartitionIndex(schema, rowValues, columns),
            PartitionType.Hash or PartitionType.LinearHash => GetHashPartitionIndex(schema, rowValues, columns),
            PartitionType.Key or PartitionType.LinearKey => GetKeyPartitionIndex(schema, rowValues, columns),
            _ => 0
        };
    }

    /// <summary>
    /// Determines which partitions to scan based on a WHERE condition (partition pruning).
    /// Returns the set of partition indices that need to be scanned.
    /// </summary>
    public HashSet<int> PrunePartitions(PartitionSchema schema, DataValue? filterValue)
    {
        var result = new HashSet<int>();

        if (filterValue == null || schema.Partitions.Count == 0)
        {
            // No pruning possible - scan all partitions
            for (int i = 0; i < schema.Partitions.Count; i++)
                result.Add(i);
            return result;
        }

        switch (schema.Type)
        {
            case PartitionType.Range:
            case PartitionType.RangeColumns:
                // For RANGE, only need partitions where LESS THAN > filterValue
                for (int i = 0; i < schema.Partitions.Count; i++)
                {
                    var part = schema.Partitions[i];
                    if (part.IsMaxValue)
                    {
                        result.Add(i);
                        break;
                    }
                    if (part.LessThanValue is long ltv)
                    {
                        if (filterValue.Value.ToLong() < ltv)
                        {
                            result.Add(i);
                            break;
                        }
                    }
                    else
                    {
                        result.Add(i);
                    }
                }
                break;

            case PartitionType.List:
            case PartitionType.ListColumns:
                // For LIST, only need the partition containing the value
                for (int i = 0; i < schema.Partitions.Count; i++)
                {
                    var part = schema.Partitions[i];
                    foreach (var val in part.InValues)
                    {
                        if (val is long lv && filterValue.Value.ToLong() == lv)
                        {
                            result.Add(i);
                        }
                        else if (val is string sv && filterValue.Value.AsString() == sv)
                        {
                            result.Add(i);
                        }
                    }
                }
                break;

            default:
                // HASH/KEY - must compute the hash
                for (int i = 0; i < schema.Partitions.Count; i++)
                    result.Add(i);
                break;
        }

        // If no partition matched, return all (safety fallback)
        if (result.Count == 0)
        {
            for (int i = 0; i < schema.Partitions.Count; i++)
                result.Add(i);
        }

        return result;
    }

    /// <summary>
    /// Adds a new partition to a RANGE-partitioned table.
    /// </summary>
    public void AddPartition(PartitionSchema schema, PartitionDefinition newPartition)
    {
        schema.Partitions.Add(newPartition);
        _logger.Info("Added partition '{0}' to table", newPartition.Name);
    }

    /// <summary>
    /// Drops a partition from a table.
    /// </summary>
    public void DropPartition(PartitionSchema schema, string partitionName)
    {
        var partition = schema.Partitions.FirstOrDefault(
            p => p.Name.Equals(partitionName, StringComparison.OrdinalIgnoreCase));
        if (partition != null)
        {
            schema.Partitions.Remove(partition);
            _logger.Info("Dropped partition '{0}'", partitionName);
        }
        else
        {
            throw new CyscaleException($"Partition '{partitionName}' does not exist");
        }
    }

    /// <summary>
    /// Reorganizes partitions (split/merge).
    /// </summary>
    public void ReorganizePartitions(PartitionSchema schema, List<string> oldPartitions, List<PartitionDefinition> newPartitions)
    {
        // Remove old partitions
        schema.Partitions.RemoveAll(p =>
            oldPartitions.Any(op => op.Equals(p.Name, StringComparison.OrdinalIgnoreCase)));
        // Add new partitions
        schema.Partitions.AddRange(newPartitions);
        _logger.Info("Reorganized {0} partition(s) into {1} partition(s)",
            oldPartitions.Count, newPartitions.Count);
    }

    private int GetRangePartitionIndex(PartitionSchema schema, DataValue[] rowValues, List<ColumnDefinition> columns)
    {
        var exprValue = EvaluatePartitionExpression(schema, rowValues, columns);
        for (int i = 0; i < schema.Partitions.Count; i++)
        {
            var part = schema.Partitions[i];
            if (part.IsMaxValue) return i;
            if (part.LessThanValue is long ltv && exprValue < ltv)
                return i;
        }
        // Should not reach here if MAXVALUE partition exists
        throw new CyscaleException("No partition found for the row value (consider adding a MAXVALUE partition)");
    }

    private int GetListPartitionIndex(PartitionSchema schema, DataValue[] rowValues, List<ColumnDefinition> columns)
    {
        var exprValue = EvaluatePartitionExpression(schema, rowValues, columns);
        for (int i = 0; i < schema.Partitions.Count; i++)
        {
            var part = schema.Partitions[i];
            foreach (var val in part.InValues)
            {
                if (val is long lv && exprValue == lv) return i;
            }
        }
        throw new CyscaleException("No partition found for the row value");
    }

    private int GetHashPartitionIndex(PartitionSchema schema, DataValue[] rowValues, List<ColumnDefinition> columns)
    {
        var numPartitions = schema.NumPartitions ?? schema.Partitions.Count;
        if (numPartitions <= 0) return 0;
        var exprValue = EvaluatePartitionExpression(schema, rowValues, columns);
        return (int)(Math.Abs(exprValue) % numPartitions);
    }

    private int GetKeyPartitionIndex(PartitionSchema schema, DataValue[] rowValues, List<ColumnDefinition> columns)
    {
        // KEY partitioning uses MySQL's internal hash function
        // We approximate with a simple hash
        var numPartitions = schema.NumPartitions ?? schema.Partitions.Count;
        if (numPartitions <= 0) return 0;

        long hash = 0;
        foreach (var colName in schema.Columns)
        {
            var colIdx = columns.FindIndex(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
            if (colIdx >= 0 && colIdx < rowValues.Length)
            {
                hash = hash * 31 + rowValues[colIdx].GetHashCode();
            }
        }
        return (int)(Math.Abs(hash) % numPartitions);
    }

    private long EvaluatePartitionExpression(PartitionSchema schema, DataValue[] rowValues, List<ColumnDefinition> columns)
    {
        // Simple evaluation: if expression is a column name, return its value
        if (schema.Expression != null)
        {
            var colIdx = columns.FindIndex(c => c.Name.Equals(schema.Expression, StringComparison.OrdinalIgnoreCase));
            if (colIdx >= 0 && colIdx < rowValues.Length)
            {
                return rowValues[colIdx].ToLong();
            }
        }
        // For COLUMNS partitioning, use the first column
        if (schema.Columns.Count > 0)
        {
            var colIdx = columns.FindIndex(c => c.Name.Equals(schema.Columns[0], StringComparison.OrdinalIgnoreCase));
            if (colIdx >= 0 && colIdx < rowValues.Length)
            {
                return rowValues[colIdx].ToLong();
            }
        }
        return 0;
    }
}
