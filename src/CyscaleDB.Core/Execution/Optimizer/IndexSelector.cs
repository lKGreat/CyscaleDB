using CyscaleDB.Core.Common;
using CyscaleDB.Core.Parsing.Ast;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Index;
using CyscaleDB.Core.Execution.Operators;

namespace CyscaleDB.Core.Execution.Optimizer;

/// <summary>
/// Selects the best index for a query based on the WHERE clause.
/// </summary>
public sealed class IndexSelector
{
    private readonly Catalog _catalog;
    private readonly IndexManager? _indexManager;
    private readonly Logger _logger;

    /// <summary>
    /// Creates a new index selector.
    /// </summary>
    /// <param name="catalog">The catalog for table information.</param>
    /// <param name="indexManager">Optional index manager for index information.</param>
    public IndexSelector(Catalog catalog, IndexManager? indexManager = null)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _indexManager = indexManager;
        _logger = LogManager.Default.GetLogger<IndexSelector>();
    }

    /// <summary>
    /// Analyzes a WHERE clause and selects the best index to use.
    /// </summary>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="whereClause">The WHERE clause expression.</param>
    /// <returns>Index selection result, or null if no index is suitable.</returns>
    public IndexSelectionResult? SelectIndex(string databaseName, string tableName, Expression? whereClause)
    {
        if (whereClause == null || _indexManager == null)
            return null;

        var schema = _catalog.GetTableSchema(databaseName, tableName);
        if (schema == null)
            return null;

        // Get all indexes for this table
        var indexes = _indexManager.GetTableIndexes(databaseName, tableName);
        if (indexes.Count == 0)
            return null;

        // Extract predicates from WHERE clause
        var predicates = ExtractPredicates(whereClause);
        if (predicates.Count == 0)
            return null;

        IndexSelectionResult? bestResult = null;
        double bestScore = 0;

        foreach (var indexInfo in indexes)
        {
            var result = EvaluateIndex(indexInfo, predicates, schema);
            if (result != null && result.Score > bestScore)
            {
                bestResult = result;
                bestScore = result.Score;
            }
        }

        if (bestResult != null)
        {
            _logger.Debug("Selected index {0} for {1}.{2} with score {3:F2}",
                bestResult.IndexInfo.IndexName, databaseName, tableName, bestResult.Score);
        }

        return bestResult;
    }

    /// <summary>
    /// Extracts simple predicates from a WHERE clause.
    /// </summary>
    private List<Predicate> ExtractPredicates(Expression expr)
    {
        var predicates = new List<Predicate>();
        ExtractPredicatesRecursive(expr, predicates);
        return predicates;
    }

    private void ExtractPredicatesRecursive(Expression expr, List<Predicate> predicates)
    {
        switch (expr)
        {
            case BinaryExpression binary:
                if (binary.Operator == BinaryOperator.And)
                {
                    // AND predicates - extract both sides
                    ExtractPredicatesRecursive(binary.Left, predicates);
                    ExtractPredicatesRecursive(binary.Right, predicates);
                }
                else if (IsComparisonOperator(binary.Operator))
                {
                    // Simple comparison predicate
                    var pred = ExtractPredicate(binary);
                    if (pred != null)
                        predicates.Add(pred);
                }
                break;

            case InExpression inExpr:
                if (inExpr.Expression is ColumnReference colRef && inExpr.Values != null)
                {
                    // col IN (v1, v2, ...)
                    predicates.Add(new Predicate
                    {
                        ColumnName = colRef.ColumnName,
                        Operator = PredicateOperator.In,
                        Values = inExpr.Values.OfType<LiteralExpression>().Select(l => l.Value).ToArray()
                    });
                }
                break;

            case BetweenExpression between:
                if (between.Expression is ColumnReference col)
                {
                    // col BETWEEN low AND high
                    var lowValue = (between.Low as LiteralExpression)?.Value;
                    var highValue = (between.High as LiteralExpression)?.Value;
                    if (lowValue.HasValue && highValue.HasValue)
                    {
                        predicates.Add(new Predicate
                        {
                            ColumnName = col.ColumnName,
                            Operator = PredicateOperator.Between,
                            Value = lowValue.Value,
                            HighValue = highValue.Value
                        });
                    }
                }
                break;

            case IsNullExpression isNull:
                if (isNull.Expression is ColumnReference nullCol)
                {
                    predicates.Add(new Predicate
                    {
                        ColumnName = nullCol.ColumnName,
                        Operator = isNull.IsNot ? PredicateOperator.IsNotNull : PredicateOperator.IsNull
                    });
                }
                break;
        }
    }

    private Predicate? ExtractPredicate(BinaryExpression binary)
    {
        // Check for col op value or value op col
        ColumnReference? colRef = null;
        LiteralExpression? literal = null;
        var op = binary.Operator;

        if (binary.Left is ColumnReference leftCol && binary.Right is LiteralExpression rightLit)
        {
            colRef = leftCol;
            literal = rightLit;
        }
        else if (binary.Right is ColumnReference rightCol && binary.Left is LiteralExpression leftLit)
        {
            colRef = rightCol;
            literal = leftLit;
            // Reverse the operator
            op = ReverseOperator(op);
        }

        if (colRef == null || literal == null)
            return null;

        return new Predicate
        {
            ColumnName = colRef.ColumnName,
            Operator = MapOperator(op),
            Value = literal.Value
        };
    }

    private static bool IsComparisonOperator(BinaryOperator op)
    {
        return op switch
        {
            BinaryOperator.Equal => true,
            BinaryOperator.NotEqual => true,
            BinaryOperator.LessThan => true,
            BinaryOperator.LessThanOrEqual => true,
            BinaryOperator.GreaterThan => true,
            BinaryOperator.GreaterThanOrEqual => true,
            _ => false
        };
    }

    private static BinaryOperator ReverseOperator(BinaryOperator op)
    {
        return op switch
        {
            BinaryOperator.LessThan => BinaryOperator.GreaterThan,
            BinaryOperator.LessThanOrEqual => BinaryOperator.GreaterThanOrEqual,
            BinaryOperator.GreaterThan => BinaryOperator.LessThan,
            BinaryOperator.GreaterThanOrEqual => BinaryOperator.LessThanOrEqual,
            _ => op
        };
    }

    private static PredicateOperator MapOperator(BinaryOperator op)
    {
        return op switch
        {
            BinaryOperator.Equal => PredicateOperator.Equal,
            BinaryOperator.NotEqual => PredicateOperator.NotEqual,
            BinaryOperator.LessThan => PredicateOperator.LessThan,
            BinaryOperator.LessThanOrEqual => PredicateOperator.LessThanOrEqual,
            BinaryOperator.GreaterThan => PredicateOperator.GreaterThan,
            BinaryOperator.GreaterThanOrEqual => PredicateOperator.GreaterThanOrEqual,
            _ => PredicateOperator.Unknown
        };
    }

    /// <summary>
    /// Evaluates how well an index can serve the given predicates.
    /// </summary>
    private IndexSelectionResult? EvaluateIndex(IndexInfo indexInfo, List<Predicate> predicates, TableSchema schema)
    {
        var indexColumns = indexInfo.Columns;
        var matchedPredicates = new List<Predicate>();
        var usedColumns = 0;

        // Check how many leading columns of the index can be matched
        foreach (var colName in indexColumns)
        {
            var pred = predicates.FirstOrDefault(p =>
                p.ColumnName.Equals(colName, StringComparison.OrdinalIgnoreCase));

            if (pred == null)
                break; // Can't use more columns after a gap

            matchedPredicates.Add(pred);
            usedColumns++;

            // After a range predicate, we can't use more columns
            if (pred.Operator != PredicateOperator.Equal && pred.Operator != PredicateOperator.In)
                break;
        }

        if (usedColumns == 0)
            return null;

        // Calculate a score based on:
        // - Number of columns matched
        // - Type of predicates (equality is better than range)
        // - Index type (hash is better for equality, btree for range)
        double score = usedColumns;

        // Bonus for equality predicates on hash index
        if (indexInfo.Type == IndexType.Hash)
        {
            if (matchedPredicates.All(p => p.Operator == PredicateOperator.Equal))
                score *= 1.5;
            else
                score *= 0.5; // Hash index is poor for range queries
        }

        // Penalty for not using all index columns
        score *= (double)usedColumns / indexColumns.Count;

        // Build the scan range
        var scanRange = BuildScanRange(matchedPredicates, indexInfo, schema);

        return new IndexSelectionResult
        {
            IndexInfo = indexInfo,
            MatchedPredicates = matchedPredicates,
            Score = score,
            ScanRange = scanRange
        };
    }

    /// <summary>
    /// Builds an index scan range from matched predicates.
    /// </summary>
    private IndexScanRange BuildScanRange(List<Predicate> predicates, IndexInfo indexInfo, TableSchema schema)
    {
        // Simple case: all equality predicates
        if (predicates.All(p => p.Operator == PredicateOperator.Equal))
        {
            var keyValues = new DataValue[predicates.Count];
            for (int i = 0; i < predicates.Count; i++)
            {
                keyValues[i] = predicates[i].Value ?? DataValue.Null;
            }
            return IndexScanRange.PointLookup(keyValues);
        }

        // Range case: build low and high bounds
        var lowValues = new DataValue[predicates.Count];
        var highValues = new DataValue[predicates.Count];
        bool lowInclusive = true;
        bool highInclusive = true;
        bool hasLowBound = false;
        bool hasHighBound = false;

        for (int i = 0; i < predicates.Count; i++)
        {
            var pred = predicates[i];

            switch (pred.Operator)
            {
                case PredicateOperator.Equal:
                    lowValues[i] = pred.Value ?? DataValue.Null;
                    highValues[i] = pred.Value ?? DataValue.Null;
                    hasLowBound = true;
                    hasHighBound = true;
                    break;

                case PredicateOperator.LessThan:
                    lowValues[i] = DataValue.Null; // Unbounded low
                    highValues[i] = pred.Value ?? DataValue.Null;
                    highInclusive = false;
                    hasHighBound = true;
                    break;

                case PredicateOperator.LessThanOrEqual:
                    lowValues[i] = DataValue.Null;
                    highValues[i] = pred.Value ?? DataValue.Null;
                    hasHighBound = true;
                    break;

                case PredicateOperator.GreaterThan:
                    lowValues[i] = pred.Value ?? DataValue.Null;
                    highValues[i] = DataValue.Null; // Unbounded high
                    lowInclusive = false;
                    hasLowBound = true;
                    break;

                case PredicateOperator.GreaterThanOrEqual:
                    lowValues[i] = pred.Value ?? DataValue.Null;
                    highValues[i] = DataValue.Null;
                    hasLowBound = true;
                    break;

                case PredicateOperator.Between:
                    lowValues[i] = pred.Value ?? DataValue.Null;
                    highValues[i] = pred.HighValue ?? DataValue.Null;
                    hasLowBound = true;
                    hasHighBound = true;
                    break;

                default:
                    lowValues[i] = DataValue.Null;
                    highValues[i] = DataValue.Null;
                    break;
            }
        }

        var lowKey = hasLowBound ? lowValues : null;
        var highKey = hasHighBound ? highValues : null;

        return IndexScanRange.Range(lowKey, highKey, lowInclusive, highInclusive);
    }
}

/// <summary>
/// Result of index selection.
/// </summary>
public sealed class IndexSelectionResult
{
    /// <summary>
    /// The selected index.
    /// </summary>
    public required IndexInfo IndexInfo { get; init; }

    /// <summary>
    /// Predicates that can be satisfied by the index.
    /// </summary>
    public required List<Predicate> MatchedPredicates { get; init; }

    /// <summary>
    /// Score indicating how good the index is for this query (higher is better).
    /// </summary>
    public required double Score { get; init; }

    /// <summary>
    /// The scan range to use with this index.
    /// </summary>
    public required IndexScanRange ScanRange { get; init; }
}

/// <summary>
/// Represents a simple predicate extracted from a WHERE clause.
/// </summary>
public sealed class Predicate
{
    /// <summary>
    /// The column name.
    /// </summary>
    public required string ColumnName { get; init; }

    /// <summary>
    /// The comparison operator.
    /// </summary>
    public required PredicateOperator Operator { get; init; }

    /// <summary>
    /// The value to compare against.
    /// </summary>
    public DataValue? Value { get; init; }

    /// <summary>
    /// The high value for BETWEEN predicates.
    /// </summary>
    public DataValue? HighValue { get; init; }

    /// <summary>
    /// Multiple values for IN predicates.
    /// </summary>
    public DataValue[]? Values { get; init; }
}

/// <summary>
/// Predicate comparison operators.
/// </summary>
public enum PredicateOperator
{
    Unknown,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    Between,
    Like,
    IsNull,
    IsNotNull
}
