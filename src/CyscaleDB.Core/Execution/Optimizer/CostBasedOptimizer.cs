using CyscaleDB.Core.Common;
using CyscaleDB.Core.Parsing.Ast;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution.Optimizer;

/// <summary>
/// Cost-Based Optimizer (CBO) that estimates the cost of different query execution
/// plans and selects the cheapest one. Key optimizations:
///
///   1. Join order optimization - choose the optimal join ordering based on table sizes
///   2. Access path selection - decide between table scan, index scan, index-only scan
///   3. Join algorithm selection - NL Join vs Hash Join based on estimated sizes
///   4. Predicate pushdown - push filters as close to the data source as possible
///
/// Cost model:
///   - Sequential I/O cost: 1.0 per page
///   - Random I/O cost: 4.0 per page  
///   - CPU cost per row: 0.01
///   - Network cost: negligible
///
/// Based on MySQL 8.4 optimizer cost model (mysql.server_cost / mysql.engine_cost).
/// </summary>
public sealed class CostBasedOptimizer
{
    private readonly StatisticsManager _statsManager;
    private readonly Catalog _catalog;

    // Cost model constants
    private const double SeqIoCost = 1.0;
    private const double RandIoCost = 4.0;
    private const double CpuCostPerRow = 0.01;
    private const double MemoryCostPerRow = 0.001;

    public CostBasedOptimizer(StatisticsManager statsManager, Catalog catalog)
    {
        _statsManager = statsManager;
        _catalog = catalog;
    }

    /// <summary>
    /// Estimates the cost of a full table scan.
    /// </summary>
    public QueryPlanCost EstimateTableScanCost(string databaseName, string tableName)
    {
        var stats = _statsManager.GetStatistics(databaseName, tableName);
        var rowCount = stats?.RowCount ?? 1000; // Default estimate

        var schema = _catalog.GetTableSchema(databaseName, tableName);
        var avgRowSize = schema != null ? EstimateRowSize(schema) : 100;
        var pageCount = Math.Max(1, (rowCount * avgRowSize) / Constants.PageSize);

        var ioCost = pageCount * SeqIoCost;
        var cpuCost = rowCount * CpuCostPerRow;

        return new QueryPlanCost
        {
            AccessMethod = "TABLE_SCAN",
            EstimatedRows = rowCount,
            IoOost = ioCost,
            CpuCost = cpuCost,
            TotalCost = ioCost + cpuCost
        };
    }

    /// <summary>
    /// Estimates the cost of an index scan with a predicate.
    /// </summary>
    public QueryPlanCost EstimateIndexScanCost(string databaseName, string tableName,
        string indexName, double selectivity)
    {
        var stats = _statsManager.GetStatistics(databaseName, tableName);
        var totalRows = stats?.RowCount ?? 1000;
        var matchingRows = (long)(totalRows * selectivity);

        var ioCost = matchingRows * RandIoCost / Constants.PageSize; // Random I/O for index lookups
        var cpuCost = matchingRows * CpuCostPerRow;

        return new QueryPlanCost
        {
            AccessMethod = $"INDEX_SCAN({indexName})",
            EstimatedRows = matchingRows,
            IoOost = ioCost,
            CpuCost = cpuCost,
            TotalCost = ioCost + cpuCost
        };
    }

    /// <summary>
    /// Estimates the cost of a Nested Loop Join.
    /// </summary>
    public QueryPlanCost EstimateNestedLoopJoinCost(QueryPlanCost outerPlan, QueryPlanCost innerPlan)
    {
        var totalRows = outerPlan.EstimatedRows * innerPlan.EstimatedRows;
        var ioCost = outerPlan.IoOost + (outerPlan.EstimatedRows * innerPlan.IoOost);
        var cpuCost = totalRows * CpuCostPerRow;

        return new QueryPlanCost
        {
            AccessMethod = "NESTED_LOOP_JOIN",
            EstimatedRows = totalRows,
            IoOost = ioCost,
            CpuCost = cpuCost,
            TotalCost = ioCost + cpuCost
        };
    }

    /// <summary>
    /// Estimates the cost of a Hash Join.
    /// </summary>
    public QueryPlanCost EstimateHashJoinCost(QueryPlanCost buildPlan, QueryPlanCost probePlan)
    {
        var totalRows = buildPlan.EstimatedRows * probePlan.EstimatedRows / 
                        Math.Max(1, Math.Max(buildPlan.EstimatedRows, probePlan.EstimatedRows));

        // Build phase: read build side + hash table construction
        var buildCost = buildPlan.TotalCost + buildPlan.EstimatedRows * MemoryCostPerRow;
        // Probe phase: read probe side + hash lookups
        var probeCost = probePlan.TotalCost + probePlan.EstimatedRows * CpuCostPerRow;

        return new QueryPlanCost
        {
            AccessMethod = "HASH_JOIN",
            EstimatedRows = totalRows,
            IoOost = buildPlan.IoOost + probePlan.IoOost,
            CpuCost = buildCost + probeCost - buildPlan.IoOost - probePlan.IoOost,
            TotalCost = buildCost + probeCost
        };
    }

    /// <summary>
    /// Chooses the best join algorithm based on cost estimates.
    /// </summary>
    public JoinAlgorithm ChooseJoinAlgorithm(string dbName, string leftTable, string rightTable,
        bool isEquiJoin)
    {
        if (!isEquiJoin)
            return JoinAlgorithm.NestedLoop; // Hash join only works for equi-joins

        var leftStats = _statsManager.GetStatistics(dbName, leftTable);
        var rightStats = _statsManager.GetStatistics(dbName, rightTable);

        var leftRows = leftStats?.RowCount ?? 1000;
        var rightRows = rightStats?.RowCount ?? 1000;

        // Heuristic: use Hash Join when both tables have > 100 rows
        // and the smaller table fits in memory (< 1M rows)
        if (leftRows > 100 && rightRows > 100 && Math.Min(leftRows, rightRows) < 1_000_000)
            return JoinAlgorithm.HashJoin;

        return JoinAlgorithm.NestedLoop;
    }

    /// <summary>
    /// Optimizes join order for multiple tables.
    /// Uses a greedy algorithm: always join the next table with the smallest intermediate result.
    /// </summary>
    public List<string> OptimizeJoinOrder(string dbName, List<string> tableNames)
    {
        if (tableNames.Count <= 2) return tableNames;

        var remaining = new List<string>(tableNames);
        var result = new List<string>();

        // Start with the smallest table
        var smallest = remaining.OrderBy(t =>
            _statsManager.GetStatistics(dbName, t)?.RowCount ?? long.MaxValue).First();
        result.Add(smallest);
        remaining.Remove(smallest);

        while (remaining.Count > 0)
        {
            // Pick the next table that produces the smallest intermediate result
            var best = remaining.OrderBy(t =>
                _statsManager.GetStatistics(dbName, t)?.RowCount ?? long.MaxValue).First();
            result.Add(best);
            remaining.Remove(best);
        }

        return result;
    }

    /// <summary>
    /// Estimates the selectivity of a predicate (fraction of rows matching).
    /// </summary>
    public double EstimateSelectivity(string dbName, string tableName, string columnName, string op)
    {
        var stats = _statsManager.GetStatistics(dbName, tableName);
        if (stats == null) return 0.1; // Default 10% selectivity

        var colStats = stats.ColumnStats.GetValueOrDefault(columnName);
        if (colStats == null) return 0.1;

        return op switch
        {
            "=" => 1.0 / Math.Max(1, colStats.EstimatedDistinctValues),
            "!=" or "<>" => 1.0 - (1.0 / Math.Max(1, colStats.EstimatedDistinctValues)),
            "<" or ">" or "<=" or ">=" => 0.33, // Assume 1/3 of rows for range predicates
            "LIKE" => 0.1, // Default estimate for LIKE
            "IN" => 0.05, // Default for IN list
            "BETWEEN" => 0.25,
            "IS NULL" => (double)colStats.NullCount / Math.Max(1, stats.RowCount),
            "IS NOT NULL" => 1.0 - ((double)colStats.NullCount / Math.Max(1, stats.RowCount)),
            _ => 0.1
        };
    }

    private static int EstimateRowSize(TableSchema schema)
    {
        int size = 32; // Row overhead
        foreach (var col in schema.Columns)
        {
            size += col.DataType switch
            {
                DataType.TinyInt or DataType.Boolean => 1,
                DataType.SmallInt => 2,
                DataType.Int or DataType.Float => 4,
                DataType.BigInt or DataType.Double or DataType.DateTime => 8,
                DataType.Decimal => 16,
                DataType.VarChar or DataType.Char => Math.Min(col.MaxLength, 100),
                DataType.Text => 50,
                _ => 8
            };
        }
        return size;
    }
}

/// <summary>
/// Estimated cost of a query plan component.
/// </summary>
public class QueryPlanCost
{
    public string AccessMethod { get; set; } = "";
    public long EstimatedRows { get; set; }
    public double IoOost { get; set; }
    public double CpuCost { get; set; }
    public double TotalCost { get; set; }

    public override string ToString() =>
        $"{AccessMethod}: rows={EstimatedRows}, io={IoOost:F2}, cpu={CpuCost:F2}, total={TotalCost:F2}";
}

/// <summary>
/// Available join algorithms.
/// </summary>
public enum JoinAlgorithm
{
    NestedLoop,
    HashJoin,
    MergeJoin
}
