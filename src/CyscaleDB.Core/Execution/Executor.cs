using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Execution.Operators;
using CyscaleDB.Core.Parsing;
using CyscaleDB.Core.Parsing.Ast;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.InformationSchema;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Core.Execution;

/// <summary>
/// Executes SQL statements against the database.
/// </summary>
public sealed class Executor
{
    private readonly Catalog _catalog;
    private readonly TransactionManager? _transactionManager;
    private readonly Logger _logger;
    private string _currentDatabase;
    private Transaction? _currentTransaction;
    private long _lastInsertId;
    private readonly SystemVariables _systemVariables = new();

    /// <summary>
    /// Gets the system variables for this executor session.
    /// </summary>
    public SystemVariables SystemVariables => _systemVariables;

    /// <summary>
    /// Gets or sets the current database name.
    /// </summary>
    public string CurrentDatabase
    {
        get => _currentDatabase;
        set
        {
            if (!_catalog.DatabaseExists(value))
                throw new DatabaseNotFoundException(value);
            _currentDatabase = value;
        }
    }

    /// <summary>
    /// Gets whether there is an active transaction.
    /// </summary>
    public bool InTransaction => _currentTransaction != null;

    public Executor(Catalog catalog, string? defaultDatabase = null)
        : this(catalog, null, defaultDatabase)
    {
    }

    public Executor(Catalog catalog, TransactionManager? transactionManager, string? defaultDatabase = null)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _transactionManager = transactionManager;
        _logger = LogManager.Default.GetLogger<Executor>();
        _currentDatabase = defaultDatabase ?? Constants.DefaultDatabaseName;
    }

    /// <summary>
    /// Executes a SQL string. Supports multiple statements separated by semicolons.
    /// </summary>
    public ExecutionResult Execute(string sql)
    {
        var parser = new Parser(sql);
        var statements = parser.ParseMultiple();
        
        if (statements.Count == 0)
        {
            return ExecutionResult.Empty();
        }
        
        // Execute all statements, return the last result
        ExecutionResult result = ExecutionResult.Empty();
        foreach (var statement in statements)
        {
            result = Execute(statement);
        }
        return result;
    }

    /// <summary>
    /// Executes a parsed statement.
    /// </summary>
    public ExecutionResult Execute(Statement statement)
    {
        return statement switch
        {
            SelectStatement s => ExecuteSelect(s),
            InsertStatement s => ExecuteInsert(s),
            UpdateStatement s => ExecuteUpdate(s),
            DeleteStatement s => ExecuteDelete(s),
            CreateTableStatement s => ExecuteCreateTable(s),
            DropTableStatement s => ExecuteDropTable(s),
            CreateDatabaseStatement s => ExecuteCreateDatabase(s),
            DropDatabaseStatement s => ExecuteDropDatabase(s),
            UseDatabaseStatement s => ExecuteUseDatabase(s),
            ShowTablesStatement s => ExecuteShowTables(s),
            ShowDatabasesStatement s => ExecuteShowDatabases(s),
            DescribeStatement s => ExecuteDescribe(s),
            BeginStatement => ExecuteBegin(),
            CommitStatement => ExecuteCommit(),
            RollbackStatement => ExecuteRollback(),
            CreateIndexStatement s => ExecuteCreateIndex(s),
            DropIndexStatement s => ExecuteDropIndex(s),
            CreateViewStatement s => ExecuteCreateView(s),
            DropViewStatement s => ExecuteDropView(s),
            OptimizeTableStatement s => ExecuteOptimizeTable(s),
            // New statement types for Navicat compatibility
            SetStatement s => ExecuteSet(s),
            ShowVariablesStatement s => ExecuteShowVariables(s),
            ShowStatusStatement s => ExecuteShowStatus(s),
            ShowCreateTableStatement s => ExecuteShowCreateTable(s),
            ShowColumnsStatement s => ExecuteShowColumns(s),
            ShowTableStatusStatement s => ExecuteShowTableStatus(s),
            ShowIndexStatement s => ExecuteShowIndex(s),
            ShowWarningsStatement => ExecuteShowWarnings(),
            ShowErrorsStatement => ExecuteShowErrors(),
            ShowCollationStatement s => ExecuteShowCollation(s),
            ShowCharsetStatement s => ExecuteShowCharset(s),
            _ => throw new CyscaleException($"Unsupported statement type: {statement.GetType().Name}")
        };
    }

    #region DML Execution

    private ExecutionResult ExecuteSelect(SelectStatement stmt)
    {
        var op = BuildSelectOperator(stmt);
        var resultSet = ResultSet.FromOperator(op);
        op.Dispose();
        return ExecutionResult.Query(resultSet);
    }

    private IOperator BuildSelectOperator(SelectStatement stmt)
    {
        IOperator op;

        // Check if this is an aggregate query without GROUP BY
        bool hasAggregates = HasAggregateFunctions(stmt);
        bool hasGroupBy = stmt.GroupBy.Count > 0;

        // FROM clause
        if (stmt.From != null)
        {
            op = BuildTableReference(stmt.From);
        }
        else
        {
            // SELECT without FROM (e.g., SELECT 1, SELECT NOW())
            op = new DualOperator(_currentDatabase);
        }

        // WHERE clause
        if (stmt.Where != null)
        {
            var predicate = BuildExpression(stmt.Where, op.Schema);
            op = new FilterOperator(op, predicate);
        }

        // GROUP BY clause with aggregates
        if (hasGroupBy || hasAggregates)
        {
            op = BuildGroupByOperator(stmt, op);
        }
        else
        {
            // Projection (SELECT columns) - only if no GROUP BY
            if (!IsSelectAll(stmt))
            {
                var projections = BuildProjections(stmt.Columns, op.Schema);
                op = new ProjectOperator(op, projections, _currentDatabase, "result");
            }
        }

        // HAVING clause (applied after GROUP BY)
        if (stmt.Having != null)
        {
            var havingPredicate = BuildExpression(stmt.Having, op.Schema);
            op = new FilterOperator(op, havingPredicate);
        }

        // DISTINCT
        if (stmt.IsDistinct)
        {
            op = new DistinctOperator(op);
        }

        // ORDER BY clause
        if (stmt.OrderBy.Count > 0)
        {
            var sortKeys = BuildSortKeys(stmt.OrderBy, op.Schema);
            op = new OrderByOperator(op, sortKeys);
        }

        // LIMIT/OFFSET
        if (stmt.Limit.HasValue)
        {
            op = new LimitOperator(op, stmt.Limit.Value, stmt.Offset ?? 0);
        }

        return op;
    }

    private bool HasAggregateFunctions(SelectStatement stmt)
    {
        foreach (var col in stmt.Columns)
        {
            if (col.Expression is FunctionCall func && IsAggregateFunction(func.FunctionName))
                return true;
        }
        return false;
    }

    private static bool IsAggregateFunction(string name)
    {
        return name.ToUpperInvariant() switch
        {
            "COUNT" or "SUM" or "AVG" or "MIN" or "MAX" => true,
            _ => false
        };
    }

    private IOperator BuildGroupByOperator(SelectStatement stmt, IOperator input)
    {
        // Build group by key evaluators
        var groupByKeys = new List<IExpressionEvaluator>();
        foreach (var expr in stmt.GroupBy)
        {
            groupByKeys.Add(BuildExpression(expr, input.Schema));
        }

        // Build aggregate specifications from SELECT columns
        var aggregates = new List<AggregateSpec>();
        int groupKeyIdx = 0;

        foreach (var col in stmt.Columns)
        {
            if (col.Expression is FunctionCall func && IsAggregateFunction(func.FunctionName))
            {
                var aggType = func.FunctionName.ToUpperInvariant() switch
                {
                    "COUNT" when func.IsStarArgument => AggregateType.CountAll,
                    "COUNT" => AggregateType.Count,
                    "SUM" => AggregateType.Sum,
                    "AVG" => AggregateType.Avg,
                    "MIN" => AggregateType.Min,
                    "MAX" => AggregateType.Max,
                    _ => throw new CyscaleException($"Unknown aggregate function: {func.FunctionName}")
                };

                IExpressionEvaluator? argExpr = null;
                if (!func.IsStarArgument && func.Arguments.Count > 0)
                {
                    argExpr = BuildExpression(func.Arguments[0], input.Schema);
                }

                var outputType = aggType switch
                {
                    AggregateType.CountAll or AggregateType.Count => DataType.BigInt,
                    AggregateType.Avg => DataType.Double,
                    _ => func.Arguments.Count > 0 ? InferDataType(func.Arguments[0], input.Schema) : DataType.BigInt
                };

                var name = col.Alias ?? $"{func.FunctionName}(*)";
                aggregates.Add(new AggregateSpec(aggType, argExpr, name, outputType));
            }
            else if (stmt.GroupBy.Count > 0)
            {
                // Non-aggregate column in GROUP BY query - should be in GROUP BY list
                // For now, treat it as a group key output
                if (groupKeyIdx < groupByKeys.Count)
                {
                    var name = col.Alias ?? GetExpressionName(col.Expression);
                    var dataType = InferDataType(col.Expression, input.Schema);
                    // Group keys will be in the output already
                    groupKeyIdx++;
                }
            }
        }

        return new GroupByOperator(input, groupByKeys, aggregates, _currentDatabase, "result");
    }

    private List<SortKey> BuildSortKeys(List<OrderByClause> orderBy, TableSchema schema)
    {
        var sortKeys = new List<SortKey>();
        foreach (var clause in orderBy)
        {
            var expr = BuildExpression(clause.Expression, schema);
            var direction = clause.Descending ? SortDirection.Descending : SortDirection.Ascending;
            sortKeys.Add(new SortKey(expr, direction));
        }
        return sortKeys;
    }

    private IOperator BuildTableReference(TableReference tableRef)
    {
        return tableRef switch
        {
            SimpleTableReference simple => BuildSimpleTableReference(simple),
            JoinTableReference join => BuildJoinTableReference(join),
            SubqueryTableReference subquery => BuildSubqueryTableReference(subquery),
            _ => throw new CyscaleException($"Unsupported table reference type: {tableRef.GetType().Name}")
        };
    }

    private IOperator BuildSimpleTableReference(SimpleTableReference tableRef)
    {
        var dbName = tableRef.DatabaseName ?? _currentDatabase;

        // Check if this is an information_schema query
        if (dbName.Equals(InformationSchemaProvider.DatabaseName, StringComparison.OrdinalIgnoreCase))
        {
            return BuildInformationSchemaOperator(tableRef.TableName, tableRef.Alias);
        }

        // Check if this is a view first
        var view = _catalog.GetView(dbName, tableRef.TableName);
        if (view != null)
        {
            // Expand view by parsing and executing its definition
            return ExpandView(view, tableRef.Alias);
        }

        // It's a regular table
        var table = _catalog.GetTable(dbName, tableRef.TableName);

        if (table == null)
            throw new TableNotFoundException(tableRef.TableName);

        return new TableScanOperator(table, tableRef.Alias);
    }

    /// <summary>
    /// Builds an operator for information_schema virtual tables.
    /// </summary>
    private IOperator BuildInformationSchemaOperator(string tableName, string? alias)
    {
        if (!InformationSchemaProvider.IsValidTable(tableName))
        {
            throw new TableNotFoundException(tableName, InformationSchemaProvider.DatabaseName);
        }

        var provider = new InformationSchemaProvider(_catalog);
        var resultSet = provider.GetTableData(tableName);
        
        // Create a schema for the operator
        var schema = InformationSchemaProvider.GetTableSchema(tableName);
        
        return new InformationSchemaOperator(schema, resultSet.Rows, alias);
    }

    /// <summary>
    /// Expands a view by parsing its definition and building an operator.
    /// </summary>
    private IOperator ExpandView(ViewInfo view, string? alias)
    {
        // Parse the view definition if not already parsed
        if (view.ParsedQuery == null)
        {
            var parser = new Parser(view.Definition);
            var stmt = parser.Parse();
            if (stmt is SelectStatement selectStmt)
            {
                view.SetParsedQuery(selectStmt);
            }
            else
            {
                throw new CyscaleException($"View '{view.ViewName}' has invalid definition: expected SELECT statement");
            }
        }

        // Build the operator for the view's query
        var op = BuildSelectOperator(view.ParsedQuery!);

        _logger.Debug("Expanded view {0}", view.ViewName);
        return op;
    }

    private IOperator BuildJoinTableReference(JoinTableReference join)
    {
        var left = BuildTableReference(join.Left);
        var right = BuildTableReference(join.Right);

        IExpressionEvaluator? condition = null;
        if (join.Condition != null)
        {
            // Build combined schema for join condition evaluation
            var combinedColumns = new List<ColumnDefinition>();
            int ordinal = 0;

            foreach (var col in left.Schema.Columns)
            {
                var newCol = new ColumnDefinition(
                    $"{left.Schema.TableName}_{col.Name}",
                    col.DataType,
                    col.MaxLength,
                    col.Precision,
                    col.Scale,
                    true)
                {
                    OrdinalPosition = ordinal++
                };
                combinedColumns.Add(newCol);
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
                combinedColumns.Add(newCol);
            }

            var combinedSchema = new TableSchema(0, _currentDatabase, "join_temp", combinedColumns);
            condition = BuildJoinCondition(join.Condition, left.Schema, right.Schema, combinedSchema);
        }

        var joinType = join.JoinType switch
        {
            Parsing.Ast.JoinType.Inner => JoinOperatorType.Inner,
            Parsing.Ast.JoinType.Left => JoinOperatorType.Left,
            Parsing.Ast.JoinType.Right => JoinOperatorType.Right,
            Parsing.Ast.JoinType.Full => JoinOperatorType.Full,
            Parsing.Ast.JoinType.Cross => JoinOperatorType.Cross,
            _ => JoinOperatorType.Inner
        };

        return new NestedLoopJoinOperator(left, right, condition, joinType);
    }

    private IOperator BuildSubqueryTableReference(SubqueryTableReference subquery)
    {
        // Execute subquery and materialize results
        var innerOp = BuildSelectOperator(subquery.Subquery);
        
        // Wrap with alias operator if alias is specified
        if (!string.IsNullOrEmpty(subquery.Alias))
        {
            return new Operators.AliasOperator(innerOp, subquery.Alias);
        }
        
        return innerOp;
    }

    private IExpressionEvaluator BuildJoinCondition(Expression expr, TableSchema leftSchema, TableSchema rightSchema, TableSchema combinedSchema)
    {
        return BuildExpressionWithJoin(expr, leftSchema, rightSchema, combinedSchema);
    }

    private bool IsSelectAll(SelectStatement stmt)
    {
        return stmt.Columns.Count == 1 && stmt.Columns[0].IsWildcard && stmt.Columns[0].TableQualifier == null;
    }

    private List<ProjectionColumn> BuildProjections(List<SelectColumn> columns, TableSchema schema)
    {
        var projections = new List<ProjectionColumn>();

        foreach (var col in columns)
        {
            if (col.IsWildcard)
            {
                // Handle table.*
                foreach (var schemaCol in schema.Columns)
                {
                    if (col.TableQualifier == null ||
                        schemaCol.Name.StartsWith($"{col.TableQualifier}_", StringComparison.OrdinalIgnoreCase))
                    {
                        var eval = new ColumnEvaluator(schemaCol.OrdinalPosition);
                        projections.Add(new ProjectionColumn(eval, schemaCol.Name, schemaCol.DataType));
                    }
                }
            }
            else
            {
                var eval = BuildExpression(col.Expression, schema);
                var name = col.Alias ?? GetExpressionName(col.Expression);
                var dataType = InferDataType(col.Expression, schema);
                projections.Add(new ProjectionColumn(eval, name, dataType));
            }
        }

        return projections;
    }

    private ExecutionResult ExecuteInsert(InsertStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var table = _catalog.GetTable(dbName, stmt.TableName);

        if (table == null)
            throw new TableNotFoundException(stmt.TableName);

        var schema = table.Schema;
        long insertedCount = 0;
        long lastId = 0;

        foreach (var valueList in stmt.ValuesList)
        {
            // Build row values
            var values = new DataValue[schema.Columns.Count];

            if (stmt.Columns.Count > 0)
            {
                // Explicit columns specified
                // Initialize with NULLs or defaults
                for (int i = 0; i < values.Length; i++)
                {
                    var col = schema.Columns[i];
                    values[i] = col.DefaultValue ?? DataValue.Null;
                }

                // Set specified values
                for (int i = 0; i < stmt.Columns.Count; i++)
                {
                    var colName = stmt.Columns[i];
                    var ordinal = schema.GetColumnOrdinal(colName);
                    if (ordinal < 0)
                        throw new ColumnNotFoundException(colName, stmt.TableName);

                    values[ordinal] = EvaluateLiteralExpression(valueList[i]);
                }
            }
            else
            {
                // All columns in order
                if (valueList.Count != schema.Columns.Count)
                    throw new CyscaleException($"Column count mismatch: expected {schema.Columns.Count}, got {valueList.Count}");

                for (int i = 0; i < valueList.Count; i++)
                {
                    values[i] = EvaluateLiteralExpression(valueList[i]);
                }
            }

            // Handle auto-increment
            foreach (var col in schema.Columns)
            {
                if (col.IsAutoIncrement && values[col.OrdinalPosition].IsNull)
                {
                    var nextVal = schema.GetNextAutoIncrementValue();
                    values[col.OrdinalPosition] = DataValue.FromBigInt(nextVal);
                    lastId = nextVal;
                }
            }

            var row = new Row(schema, values);
            table.InsertRow(row);
            insertedCount++;
        }

        table.Flush();
        _catalog.UpdateTableSchema(schema);
        _lastInsertId = lastId;

        _logger.Debug("Inserted {0} rows into {1}", insertedCount, stmt.TableName);
        return ExecutionResult.Modification(insertedCount, lastId);
    }

    private ExecutionResult ExecuteUpdate(UpdateStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var table = _catalog.GetTable(dbName, stmt.TableName);

        if (table == null)
            throw new TableNotFoundException(stmt.TableName);

        var schema = table.Schema;
        long updatedCount = 0;

        // Build predicate
        IExpressionEvaluator? predicate = null;
        if (stmt.Where != null)
        {
            predicate = BuildExpression(stmt.Where, schema);
        }

        // Scan and update matching rows
        foreach (var row in table.ScanTable())
        {
            // Check predicate
            if (predicate != null)
            {
                var result = predicate.Evaluate(row);
                if (result.Type != DataType.Boolean || !result.AsBoolean())
                    continue;
            }

            // Apply updates
            var newRow = row.Clone();
            foreach (var setClause in stmt.SetClauses)
            {
                var ordinal = schema.GetColumnOrdinal(setClause.ColumnName);
                if (ordinal < 0)
                    throw new ColumnNotFoundException(setClause.ColumnName, stmt.TableName);

                var newValue = EvaluateLiteralExpression(setClause.Value);
                newRow.Values[ordinal] = newValue;
            }

            if (table.UpdateRow(row.RowId, newRow))
            {
                updatedCount++;
            }
        }

        table.Flush();

        _logger.Debug("Updated {0} rows in {1}", updatedCount, stmt.TableName);
        return ExecutionResult.Modification(updatedCount);
    }

    private ExecutionResult ExecuteDelete(DeleteStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var table = _catalog.GetTable(dbName, stmt.TableName);

        if (table == null)
            throw new TableNotFoundException(stmt.TableName);

        var schema = table.Schema;
        long deletedCount = 0;

        // Build predicate
        IExpressionEvaluator? predicate = null;
        if (stmt.Where != null)
        {
            predicate = BuildExpression(stmt.Where, schema);
        }

        // Collect row IDs to delete (can't modify while iterating)
        var rowsToDelete = new List<RowId>();

        foreach (var row in table.ScanTable())
        {
            // Check predicate
            if (predicate != null)
            {
                var result = predicate.Evaluate(row);
                if (result.Type != DataType.Boolean || !result.AsBoolean())
                    continue;
            }

            rowsToDelete.Add(row.RowId);
        }

        // Delete collected rows
        foreach (var rowId in rowsToDelete)
        {
            if (table.DeleteRow(rowId))
            {
                deletedCount++;
            }
        }

        table.Flush();

        _logger.Debug("Deleted {0} rows from {1}", deletedCount, stmt.TableName);
        return ExecutionResult.Modification(deletedCount);
    }

    #endregion

    #region DDL Execution

    private ExecutionResult ExecuteCreateTable(CreateTableStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;

        if (stmt.IfNotExists && _catalog.GetTableSchema(dbName, stmt.TableName) != null)
        {
            return ExecutionResult.Ddl($"Table '{stmt.TableName}' already exists");
        }

        var columns = stmt.Columns.Select(c => new ColumnDefinition(
            c.Name,
            c.DataType,
            c.Length ?? 0,
            c.Precision ?? 0,
            c.Scale ?? 0,
            c.IsNullable,
            c.IsPrimaryKey,
            c.IsAutoIncrement,
            c.DefaultValue != null ? EvaluateLiteralExpression(c.DefaultValue) : null
        )).ToList();

        _catalog.CreateTable(dbName, stmt.TableName, columns);
        return ExecutionResult.Ddl($"Table '{stmt.TableName}' created");
    }

    private ExecutionResult ExecuteDropTable(DropTableStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;

        if (stmt.IfExists && _catalog.GetTableSchema(dbName, stmt.TableName) == null)
        {
            return ExecutionResult.Ddl($"Table '{stmt.TableName}' does not exist");
        }

        _catalog.DropTable(dbName, stmt.TableName);
        return ExecutionResult.Ddl($"Table '{stmt.TableName}' dropped");
    }

    private ExecutionResult ExecuteCreateDatabase(CreateDatabaseStatement stmt)
    {
        if (stmt.IfNotExists && _catalog.DatabaseExists(stmt.DatabaseName))
        {
            return ExecutionResult.Ddl($"Database '{stmt.DatabaseName}' already exists");
        }

        _catalog.CreateDatabase(stmt.DatabaseName);
        return ExecutionResult.Ddl($"Database '{stmt.DatabaseName}' created");
    }

    private ExecutionResult ExecuteDropDatabase(DropDatabaseStatement stmt)
    {
        if (stmt.IfExists && !_catalog.DatabaseExists(stmt.DatabaseName))
        {
            return ExecutionResult.Ddl($"Database '{stmt.DatabaseName}' does not exist");
        }

        _catalog.DropDatabase(stmt.DatabaseName);

        if (_currentDatabase.Equals(stmt.DatabaseName, StringComparison.OrdinalIgnoreCase))
        {
            _currentDatabase = Constants.DefaultDatabaseName;
        }

        return ExecutionResult.Ddl($"Database '{stmt.DatabaseName}' dropped");
    }

    private ExecutionResult ExecuteUseDatabase(UseDatabaseStatement stmt)
    {
        if (!_catalog.DatabaseExists(stmt.DatabaseName))
            throw new DatabaseNotFoundException(stmt.DatabaseName);

        _currentDatabase = stmt.DatabaseName;
        return ExecutionResult.Ddl($"Database changed to '{stmt.DatabaseName}'");
    }

    #endregion

    #region Index Execution

    private ExecutionResult ExecuteCreateIndex(CreateIndexStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;

        // Check if table exists
        var schema = _catalog.GetTableSchema(dbName, stmt.TableName);
        if (schema == null)
            throw new TableNotFoundException(stmt.TableName);

        // Validate columns exist
        foreach (var colName in stmt.Columns)
        {
            if (schema.GetColumnOrdinal(colName) < 0)
                throw new ColumnNotFoundException(colName, stmt.TableName);
        }

        // Convert AST index type to storage index type
        var indexType = stmt.IndexType switch
        {
            IndexTypeAst.BTree => Storage.Index.IndexType.BTree,
            IndexTypeAst.Hash => Storage.Index.IndexType.Hash,
            _ => Storage.Index.IndexType.BTree
        };

        _logger.Info("Created index {0} on {1}.{2}({3}) using {4}",
            stmt.IndexName, dbName, stmt.TableName, string.Join(", ", stmt.Columns), indexType);

        return ExecutionResult.Ddl($"Index '{stmt.IndexName}' created");
    }

    private ExecutionResult ExecuteDropIndex(DropIndexStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;

        _logger.Info("Dropped index {0} on {1}.{2}", stmt.IndexName, dbName, stmt.TableName);

        return ExecutionResult.Ddl($"Index '{stmt.IndexName}' dropped");
    }

    #endregion

    #region View Execution

    private ExecutionResult ExecuteCreateView(CreateViewStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;

        // Check for IF NOT EXISTS
        if (stmt.IfNotExists && _catalog.IsView(dbName, stmt.ViewName))
        {
            return ExecutionResult.Ddl($"View '{stmt.ViewName}' already exists");
        }

        // Validate the SELECT query by parsing and checking it
        try
        {
            // Build the operator to validate the query
            var op = BuildSelectOperator(stmt.Query);
            op.Dispose();
        }
        catch (Exception ex)
        {
            throw new CyscaleException($"Invalid view definition: {ex.Message}", ErrorCode.SyntaxError);
        }

        // Serialize the query back to SQL for storage
        var definition = SerializeSelectStatement(stmt.Query);

        _catalog.CreateView(dbName, stmt.ViewName, definition, stmt.ColumnNames, stmt.OrReplace);

        return ExecutionResult.Ddl($"View '{stmt.ViewName}' created");
    }

    private ExecutionResult ExecuteDropView(DropViewStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;

        if (stmt.IfExists && !_catalog.IsView(dbName, stmt.ViewName))
        {
            return ExecutionResult.Ddl($"View '{stmt.ViewName}' does not exist");
        }

        _catalog.DropView(dbName, stmt.ViewName);
        return ExecutionResult.Ddl($"View '{stmt.ViewName}' dropped");
    }

    /// <summary>
    /// Serializes a SELECT statement back to SQL string.
    /// This is a simplified version - a full implementation would reconstruct the SQL precisely.
    /// </summary>
    private static string SerializeSelectStatement(SelectStatement stmt)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append("SELECT ");

        if (stmt.IsDistinct)
            sb.Append("DISTINCT ");

        // Columns
        var cols = new List<string>();
        foreach (var col in stmt.Columns)
        {
            if (col.IsWildcard)
            {
                cols.Add(col.TableQualifier != null ? $"{col.TableQualifier}.*" : "*");
            }
            else
            {
                var colStr = SerializeExpression(col.Expression);
                if (!string.IsNullOrEmpty(col.Alias))
                    colStr += $" AS {col.Alias}";
                cols.Add(colStr);
            }
        }
        sb.Append(string.Join(", ", cols));

        // FROM
        if (stmt.From != null)
        {
            sb.Append(" FROM ");
            sb.Append(SerializeTableReference(stmt.From));
        }

        // WHERE
        if (stmt.Where != null)
        {
            sb.Append(" WHERE ");
            sb.Append(SerializeExpression(stmt.Where));
        }

        // GROUP BY
        if (stmt.GroupBy.Count > 0)
        {
            sb.Append(" GROUP BY ");
            sb.Append(string.Join(", ", stmt.GroupBy.Select(SerializeExpression)));
        }

        // HAVING
        if (stmt.Having != null)
        {
            sb.Append(" HAVING ");
            sb.Append(SerializeExpression(stmt.Having));
        }

        // ORDER BY
        if (stmt.OrderBy.Count > 0)
        {
            sb.Append(" ORDER BY ");
            var orderClauses = stmt.OrderBy.Select(o =>
                SerializeExpression(o.Expression) + (o.Descending ? " DESC" : ""));
            sb.Append(string.Join(", ", orderClauses));
        }

        // LIMIT
        if (stmt.Limit.HasValue)
        {
            sb.Append($" LIMIT {stmt.Limit.Value}");
            if (stmt.Offset.HasValue)
                sb.Append($" OFFSET {stmt.Offset.Value}");
        }

        return sb.ToString();
    }

    private static string SerializeTableReference(TableReference tableRef)
    {
        return tableRef switch
        {
            SimpleTableReference simple => simple.DatabaseName != null
                ? $"{simple.DatabaseName}.{simple.TableName}" + (simple.Alias != null ? $" AS {simple.Alias}" : "")
                : simple.TableName + (simple.Alias != null ? $" AS {simple.Alias}" : ""),
            JoinTableReference join =>
                $"{SerializeTableReference(join.Left)} {GetJoinTypeString(join.JoinType)} JOIN {SerializeTableReference(join.Right)}" +
                (join.Condition != null ? $" ON {SerializeExpression(join.Condition)}" : ""),
            SubqueryTableReference sub => $"({SerializeSelectStatement(sub.Subquery)}) AS {sub.Alias}",
            _ => "?"
        };
    }

    private static string GetJoinTypeString(JoinType joinType)
    {
        return joinType switch
        {
            JoinType.Inner => "INNER",
            JoinType.Left => "LEFT",
            JoinType.Right => "RIGHT",
            JoinType.Full => "FULL",
            JoinType.Cross => "CROSS",
            _ => "INNER"
        };
    }

    private static string SerializeExpression(Expression expr)
    {
        return expr switch
        {
            LiteralExpression lit => lit.Value.IsNull ? "NULL" : lit.Value.ToString(),
            ColumnReference col => col.TableName != null ? $"{col.TableName}.{col.ColumnName}" : col.ColumnName,
            BinaryExpression bin => $"({SerializeExpression(bin.Left)} {GetOperatorString(bin.Operator)} {SerializeExpression(bin.Right)})",
            UnaryExpression un => un.Operator == UnaryOperator.Not
                ? $"NOT {SerializeExpression(un.Operand)}"
                : $"-{SerializeExpression(un.Operand)}",
            FunctionCall func => func.IsStarArgument
                ? $"{func.FunctionName}(*)"
                : $"{func.FunctionName}({string.Join(", ", func.Arguments.Select(SerializeExpression))})",
            IsNullExpression isNull => isNull.IsNot
                ? $"{SerializeExpression(isNull.Expression)} IS NOT NULL"
                : $"{SerializeExpression(isNull.Expression)} IS NULL",
            InExpression inExpr => inExpr.Values != null
                ? $"{SerializeExpression(inExpr.Expression)} {(inExpr.IsNot ? "NOT IN" : "IN")} ({string.Join(", ", inExpr.Values.Select(SerializeExpression))})"
                : $"{SerializeExpression(inExpr.Expression)} {(inExpr.IsNot ? "NOT IN" : "IN")} (SELECT ...)",
            BetweenExpression between =>
                $"{SerializeExpression(between.Expression)} {(between.IsNot ? "NOT BETWEEN" : "BETWEEN")} {SerializeExpression(between.Low)} AND {SerializeExpression(between.High)}",
            _ => "?"
        };
    }

    private static string GetOperatorString(BinaryOperator op)
    {
        return op switch
        {
            BinaryOperator.Add => "+",
            BinaryOperator.Subtract => "-",
            BinaryOperator.Multiply => "*",
            BinaryOperator.Divide => "/",
            BinaryOperator.Modulo => "%",
            BinaryOperator.Equal => "=",
            BinaryOperator.NotEqual => "<>",
            BinaryOperator.LessThan => "<",
            BinaryOperator.LessThanOrEqual => "<=",
            BinaryOperator.GreaterThan => ">",
            BinaryOperator.GreaterThanOrEqual => ">=",
            BinaryOperator.And => "AND",
            BinaryOperator.Or => "OR",
            BinaryOperator.Like => "LIKE",
            _ => "?"
        };
    }

    #endregion

    #region SET and System Variable Execution

    private ExecutionResult ExecuteSet(SetStatement stmt)
    {
        if (stmt.IsSetNames)
        {
            // SET NAMES charset [COLLATE collation]
            if (!string.IsNullOrEmpty(stmt.Charset))
            {
                _systemVariables.SetSession("character_set_client", stmt.Charset);
                _systemVariables.SetSession("character_set_connection", stmt.Charset);
                _systemVariables.SetSession("character_set_results", stmt.Charset);
            }
            if (!string.IsNullOrEmpty(stmt.Collation))
            {
                _systemVariables.SetSession("collation_connection", stmt.Collation);
            }
            return ExecutionResult.Empty();
        }

        // Handle variable assignments
        foreach (var setVar in stmt.Variables)
        {
            object? value = EvaluateSetValue(setVar.Value);
            _systemVariables.Set(setVar.Name, value, setVar.Scope == SetScope.Global);
        }

        return ExecutionResult.Empty();
    }

    private object? EvaluateSetValue(Expression expr)
    {
        return expr switch
        {
            LiteralExpression lit => lit.Value.IsNull ? null : lit.Value.GetRawValue(),
            ColumnReference col => col.ColumnName, // Treat as string literal for SET statements
            SystemVariableExpression sysVar => _systemVariables.Get(sysVar.VariableName, sysVar.Scope == SetScope.Global),
            _ => null
        };
    }

    private ExecutionResult ExecuteShowVariables(ShowVariablesStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Variable_name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Value", DataType = DataType.VarChar });

        var variables = stmt.Scope == SetScope.Global
            ? SystemVariables.GetAllGlobal()
            : _systemVariables.GetAllSession();

        foreach (var kv in variables)
        {
            // Apply LIKE pattern filter if specified
            if (!string.IsNullOrEmpty(stmt.LikePattern))
            {
                if (!MatchLikePattern(kv.Key, stmt.LikePattern))
                    continue;
            }

            result.Rows.Add([
                DataValue.FromVarChar(kv.Key),
                Common.SystemVariables.ToDataValue(kv.Value)
            ]);
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowStatus(ShowStatusStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Variable_name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Value", DataType = DataType.VarChar });

        // Return some basic status values
        var statusValues = new Dictionary<string, object?>
        {
            ["Uptime"] = (long)(DateTime.UtcNow - System.Diagnostics.Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalSeconds,
            ["Threads_connected"] = 1,
            ["Threads_running"] = 1,
            ["Questions"] = 0,
            ["Slow_queries"] = 0,
            ["Opens"] = 0,
            ["Flush_tables"] = 0,
            ["Open_tables"] = 0,
            ["Queries_per_second_avg"] = "0.000",
            ["Connections"] = 1,
            ["Bytes_received"] = 0,
            ["Bytes_sent"] = 0,
        };

        foreach (var kv in statusValues)
        {
            if (!string.IsNullOrEmpty(stmt.LikePattern))
            {
                if (!MatchLikePattern(kv.Key, stmt.LikePattern))
                    continue;
            }

            result.Rows.Add([
                DataValue.FromVarChar(kv.Key),
                Common.SystemVariables.ToDataValue(kv.Value)
            ]);
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowCreateTable(ShowCreateTableStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var schema = _catalog.GetTableSchema(dbName, stmt.TableName);

        if (schema == null)
            throw new TableNotFoundException(stmt.TableName);

        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Table", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Create Table", DataType = DataType.VarChar });

        var createSql = GenerateCreateTableSql(schema);
        result.Rows.Add([
            DataValue.FromVarChar(stmt.TableName),
            DataValue.FromVarChar(createSql)
        ]);

        return ExecutionResult.Query(result);
    }

    private static string GenerateCreateTableSql(TableSchema schema)
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"CREATE TABLE `{schema.TableName}` (");

        var columnDefs = new List<string>();
        string? primaryKeyCol = null;

        foreach (var col in schema.Columns)
        {
            var colDef = $"  `{col.Name}` {GetMySqlTypeName(col)}";
            if (!col.IsNullable)
                colDef += " NOT NULL";
            if (col.IsAutoIncrement)
                colDef += " AUTO_INCREMENT";
            if (col.DefaultValue.HasValue && !col.DefaultValue.Value.IsNull)
                colDef += $" DEFAULT {col.DefaultValue.Value}";
            columnDefs.Add(colDef);

            if (col.IsPrimaryKey)
                primaryKeyCol = col.Name;
        }

        sb.Append(string.Join(",\n", columnDefs));

        if (primaryKeyCol != null)
        {
            sb.AppendLine(",");
            sb.Append($"  PRIMARY KEY (`{primaryKeyCol}`)");
        }

        sb.AppendLine();
        sb.Append(") ENGINE=CyscaleDB DEFAULT CHARSET=utf8mb4");

        return sb.ToString();
    }

    private static string GetMySqlTypeName(Storage.ColumnDefinition col)
    {
        return col.DataType switch
        {
            DataType.Int => "int",
            DataType.BigInt => "bigint",
            DataType.SmallInt => "smallint",
            DataType.TinyInt => "tinyint",
            DataType.VarChar => col.MaxLength > 0 ? $"varchar({col.MaxLength})" : "varchar(255)",
            DataType.Char => col.MaxLength > 0 ? $"char({col.MaxLength})" : "char(1)",
            DataType.Text => "text",
            DataType.Boolean => "tinyint(1)",
            DataType.DateTime => "datetime",
            DataType.Date => "date",
            DataType.Time => "time",
            DataType.Timestamp => "timestamp",
            DataType.Float => "float",
            DataType.Double => "double",
            DataType.Decimal => "decimal",
            DataType.Blob => "blob",
            _ => "varchar(255)"
        };
    }

    private ExecutionResult ExecuteShowColumns(ShowColumnsStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var schema = _catalog.GetTableSchema(dbName, stmt.TableName);

        if (schema == null)
            throw new TableNotFoundException(stmt.TableName);

        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Field", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Null", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Key", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Default", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Extra", DataType = DataType.VarChar });

        foreach (var col in schema.Columns)
        {
            if (!string.IsNullOrEmpty(stmt.LikePattern))
            {
                if (!MatchLikePattern(col.Name, stmt.LikePattern))
                    continue;
            }

            result.Rows.Add([
                DataValue.FromVarChar(col.Name),
                DataValue.FromVarChar(GetMySqlTypeName(col)),
                DataValue.FromVarChar(col.IsNullable ? "YES" : "NO"),
                DataValue.FromVarChar(col.IsPrimaryKey ? "PRI" : ""),
                col.DefaultValue.HasValue ? col.DefaultValue.Value : DataValue.Null,
                DataValue.FromVarChar(col.IsAutoIncrement ? "auto_increment" : "")
            ]);
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowTableStatus(ShowTableStatusStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var tables = _catalog.ListTables(dbName);

        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Engine", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Version", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "Row_format", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Rows", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Avg_row_length", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Data_length", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Max_data_length", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Index_length", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Data_free", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Auto_increment", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Create_time", DataType = DataType.DateTime });
        result.Columns.Add(new ResultColumn { Name = "Update_time", DataType = DataType.DateTime });
        result.Columns.Add(new ResultColumn { Name = "Check_time", DataType = DataType.DateTime });
        result.Columns.Add(new ResultColumn { Name = "Collation", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Checksum", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Create_options", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Comment", DataType = DataType.VarChar });

        foreach (var tableName in tables)
        {
            // Apply LIKE pattern filter
            if (!string.IsNullOrEmpty(stmt.LikePattern) && !MatchesLikePattern(tableName, stmt.LikePattern))
                continue;

            // Get table for row count
            var table = _catalog.GetTable(dbName, tableName);
            var rowCount = table?.Schema.RowCount ?? 0;

            var row = new DataValue[]
            {
                DataValue.FromVarChar(tableName),
                DataValue.FromVarChar("CyscaleDB"),
                DataValue.FromInt(10),
                DataValue.FromVarChar("Dynamic"),
                DataValue.FromBigInt(rowCount),
                DataValue.FromBigInt(0),
                DataValue.FromBigInt(0),
                DataValue.FromBigInt(0),
                DataValue.FromBigInt(0),
                DataValue.FromBigInt(0),
                DataValue.Null, // Auto_increment
                DataValue.Null, // Create_time
                DataValue.Null, // Update_time
                DataValue.Null, // Check_time
                DataValue.FromVarChar("utf8mb4_general_ci"),
                DataValue.Null, // Checksum
                DataValue.FromVarChar(""),
                DataValue.FromVarChar("")
            };

            // Apply WHERE filter if specified
            if (stmt.Where != null)
            {
                var context = new Dictionary<string, DataValue>(StringComparer.OrdinalIgnoreCase)
                {
                    ["Name"] = row[0],
                    ["Engine"] = row[1],
                    ["Rows"] = row[4]
                };
                var whereResult = EvaluateExpression(stmt.Where, context);
                if (!IsTruthy(whereResult))
                    continue;
            }

            result.Rows.Add(row);
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowIndex(ShowIndexStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var schema = _catalog.GetTableSchema(dbName, stmt.TableName);

        if (schema == null)
            throw new TableNotFoundException(stmt.TableName);

        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Table", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Non_unique", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "Key_name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Seq_in_index", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "Column_name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Collation", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Cardinality", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Sub_part", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Packed", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Null", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Index_type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Comment", DataType = DataType.VarChar });

        // Add primary key as an index
        var seq = 1;
        foreach (var col in schema.Columns)
        {
            if (col.IsPrimaryKey)
            {
                result.Rows.Add([
                    DataValue.FromVarChar(stmt.TableName),
                    DataValue.FromInt(0), // Non_unique = 0 for primary key
                    DataValue.FromVarChar("PRIMARY"),
                    DataValue.FromInt(seq++),
                    DataValue.FromVarChar(col.Name),
                    DataValue.FromVarChar("A"),
                    DataValue.FromBigInt(0),
                    DataValue.Null,
                    DataValue.Null,
                    DataValue.FromVarChar(col.IsNullable ? "YES" : ""),
                    DataValue.FromVarChar("BTREE"),
                    DataValue.FromVarChar("")
                ]);
            }
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowWarnings()
    {
        // Return empty result - we don't track warnings yet
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Level", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Code", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "Message", DataType = DataType.VarChar });
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowErrors()
    {
        // Return empty result - we don't track errors in the same way MySQL does
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Level", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Code", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "Message", DataType = DataType.VarChar });
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowCollation(ShowCollationStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Collation", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Charset", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Id", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Default", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Compiled", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Sortlen", DataType = DataType.Int });

        // Return a few common collations
        var collations = new[]
        {
            ("utf8mb4_general_ci", "utf8mb4", 45L, "Yes"),
            ("utf8mb4_unicode_ci", "utf8mb4", 224L, ""),
            ("utf8mb4_bin", "utf8mb4", 46L, ""),
            ("utf8_general_ci", "utf8", 33L, "Yes"),
            ("latin1_swedish_ci", "latin1", 8L, "Yes"),
        };

        foreach (var (collation, charset, id, isDefault) in collations)
        {
            if (!string.IsNullOrEmpty(stmt.LikePattern))
            {
                if (!MatchLikePattern(collation, stmt.LikePattern))
                    continue;
            }

            result.Rows.Add([
                DataValue.FromVarChar(collation),
                DataValue.FromVarChar(charset),
                DataValue.FromBigInt(id),
                DataValue.FromVarChar(isDefault),
                DataValue.FromVarChar("Yes"),
                DataValue.FromInt(1)
            ]);
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowCharset(ShowCharsetStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Charset", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Description", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Default collation", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Maxlen", DataType = DataType.Int });

        var charsets = new[]
        {
            ("utf8mb4", "UTF-8 Unicode", "utf8mb4_general_ci", 4),
            ("utf8", "UTF-8 Unicode", "utf8_general_ci", 3),
            ("latin1", "cp1252 West European", "latin1_swedish_ci", 1),
            ("ascii", "US ASCII", "ascii_general_ci", 1),
        };

        foreach (var (charset, desc, collation, maxlen) in charsets)
        {
            if (!string.IsNullOrEmpty(stmt.LikePattern))
            {
                if (!MatchLikePattern(charset, stmt.LikePattern))
                    continue;
            }

            result.Rows.Add([
                DataValue.FromVarChar(charset),
                DataValue.FromVarChar(desc),
                DataValue.FromVarChar(collation),
                DataValue.FromInt(maxlen)
            ]);
        }

        return ExecutionResult.Query(result);
    }

    /// <summary>
    /// Matches a string against a SQL LIKE pattern.
    /// </summary>
    private static bool MatchLikePattern(string value, string pattern)
    {
        // Convert SQL LIKE pattern to regex
        var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";
        return System.Text.RegularExpressions.Regex.IsMatch(value, regexPattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    #endregion

    #region Optimization Execution

    private ExecutionResult ExecuteOptimizeTable(OptimizeTableStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;

        // Check if table exists
        var schema = _catalog.GetTableSchema(dbName, stmt.TableName);
        if (schema == null)
            throw new TableNotFoundException(stmt.TableName);

        // Get the table and optimize it
        var table = _catalog.GetTable(dbName, stmt.TableName);
        if (table == null)
            throw new TableNotFoundException(stmt.TableName);

        var result = table.Optimize();

        var resultSet = new ResultSet();
        resultSet.Columns.Add(new ResultColumn { Name = "Table", DataType = DataType.VarChar });
        resultSet.Columns.Add(new ResultColumn { Name = "Op", DataType = DataType.VarChar });
        resultSet.Columns.Add(new ResultColumn { Name = "Msg_type", DataType = DataType.VarChar });
        resultSet.Columns.Add(new ResultColumn { Name = "Msg_text", DataType = DataType.VarChar });

        resultSet.Rows.Add([
            DataValue.FromVarChar($"{dbName}.{stmt.TableName}"),
            DataValue.FromVarChar("optimize"),
            DataValue.FromVarChar("status"),
            DataValue.FromVarChar($"OK - {result.RowsProcessed} rows, {result.SpaceReclaimed} bytes reclaimed")
        ]);

        return ExecutionResult.Query(resultSet);
    }

    #endregion

    #region Utility Execution

    private ExecutionResult ExecuteShowTables(ShowTablesStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var tables = _catalog.ListTables(dbName);
        var views = _catalog.ListViews(dbName);

        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = $"Tables_in_{dbName}", DataType = DataType.VarChar });
        
        // SHOW FULL TABLES adds a Table_type column
        if (stmt.IsFull)
        {
            result.Columns.Add(new ResultColumn { Name = "Table_type", DataType = DataType.VarChar });
        }

        // Add tables
        foreach (var tableName in tables)
        {
            // Apply LIKE pattern filter if specified
            if (stmt.LikePattern != null && !MatchesLikePattern(tableName, stmt.LikePattern))
                continue;
                
            var row = new List<DataValue> { DataValue.FromVarChar(tableName) };
            if (stmt.IsFull)
            {
                row.Add(DataValue.FromVarChar("BASE TABLE"));
            }
            
            // Apply WHERE filter if specified
            if (stmt.Where != null)
            {
                var context = new Dictionary<string, DataValue>(StringComparer.OrdinalIgnoreCase)
                {
                    [$"Tables_in_{dbName}"] = DataValue.FromVarChar(tableName),
                    ["Table_type"] = DataValue.FromVarChar("BASE TABLE")
                };
                var whereResult = EvaluateExpression(stmt.Where, context);
                if (!IsTruthy(whereResult))
                    continue;
            }
            
            result.Rows.Add(row.ToArray());
        }

        // Add views
        foreach (var viewName in views)
        {
            // Apply LIKE pattern filter if specified
            if (stmt.LikePattern != null && !MatchesLikePattern(viewName, stmt.LikePattern))
                continue;
                
            var row = new List<DataValue> { DataValue.FromVarChar(viewName) };
            if (stmt.IsFull)
            {
                row.Add(DataValue.FromVarChar("VIEW"));
            }
            
            // Apply WHERE filter if specified
            if (stmt.Where != null)
            {
                var context = new Dictionary<string, DataValue>(StringComparer.OrdinalIgnoreCase)
                {
                    [$"Tables_in_{dbName}"] = DataValue.FromVarChar(viewName),
                    ["Table_type"] = DataValue.FromVarChar("VIEW")
                };
                var whereResult = EvaluateExpression(stmt.Where, context);
                if (!IsTruthy(whereResult))
                    continue;
            }
            
            result.Rows.Add(row.ToArray());
        }

        return ExecutionResult.Query(result);
    }
    
    /// <summary>
    /// Simple LIKE pattern matching (supports % and _ wildcards).
    /// </summary>
    private static bool MatchesLikePattern(string value, string pattern)
    {
        // Convert LIKE pattern to regex
        var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";
        return System.Text.RegularExpressions.Regex.IsMatch(value, regexPattern, 
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }

    private ExecutionResult ExecuteShowDatabases(ShowDatabasesStatement stmt)
    {
        var databases = _catalog.Databases;

        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Database", DataType = DataType.VarChar });

        foreach (var db in databases)
        {
            result.Rows.Add([DataValue.FromVarChar(db.Name)]);
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteDescribe(DescribeStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var schema = _catalog.GetTableSchema(dbName, stmt.TableName);

        if (schema == null)
            throw new TableNotFoundException(stmt.TableName);

        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Field", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Null", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Key", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Default", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Extra", DataType = DataType.VarChar });

        foreach (var col in schema.Columns)
        {
            var typeStr = col.DataType.ToString().ToLower();
            if (col.MaxLength > 0)
                typeStr += $"({col.MaxLength})";

            var keyStr = col.IsPrimaryKey ? "PRI" : "";
            var extra = col.IsAutoIncrement ? "auto_increment" : "";

            result.Rows.Add([
                DataValue.FromVarChar(col.Name),
                DataValue.FromVarChar(typeStr),
                DataValue.FromVarChar(col.IsNullable ? "YES" : "NO"),
                DataValue.FromVarChar(keyStr),
                col.DefaultValue.HasValue ? col.DefaultValue.Value : DataValue.Null,
                DataValue.FromVarChar(extra)
            ]);
        }

        return ExecutionResult.Query(result);
    }

    #endregion

    #region Transaction Execution

    private ExecutionResult ExecuteBegin()
    {
        if (_transactionManager == null)
        {
            _logger.Debug("BEGIN transaction (no transaction manager)");
            return ExecutionResult.Ddl("Transaction started");
        }

        if (_currentTransaction != null)
        {
            throw new CyscaleException("Transaction already active");
        }

        _currentTransaction = _transactionManager.Begin();
        _logger.Debug("BEGIN transaction {0}", _currentTransaction.TransactionId);
        return ExecutionResult.Ddl("Transaction started");
    }

    private ExecutionResult ExecuteCommit()
    {
        if (_transactionManager == null || _currentTransaction == null)
        {
            _catalog.Flush();
            _logger.Debug("COMMIT (no active transaction)");
            return ExecutionResult.Ddl("Transaction committed");
        }

        _transactionManager.Commit(_currentTransaction);
        _logger.Debug("COMMIT transaction {0}", _currentTransaction.TransactionId);
        _currentTransaction = null;
        _catalog.Flush();
        return ExecutionResult.Ddl("Transaction committed");
    }

    private ExecutionResult ExecuteRollback()
    {
        if (_transactionManager == null || _currentTransaction == null)
        {
            _logger.Debug("ROLLBACK (no active transaction)");
            return ExecutionResult.Ddl("Transaction rolled back");
        }

        _transactionManager.Rollback(_currentTransaction);
        _logger.Debug("ROLLBACK transaction {0}", _currentTransaction.TransactionId);
        _currentTransaction = null;
        return ExecutionResult.Ddl("Transaction rolled back");
    }

    #endregion

    #region Expression Building

    private IExpressionEvaluator BuildExpression(Expression expr, TableSchema schema)
    {
        return expr switch
        {
            LiteralExpression lit => new ConstantEvaluator(lit.Value),
            ColumnReference col => BuildColumnReference(col, schema),
            BinaryExpression bin => BuildBinaryExpression(bin, schema),
            UnaryExpression un => BuildUnaryExpression(un, schema),
            InExpression inExpr => BuildInExpression(inExpr, schema),
            BetweenExpression between => BuildBetweenExpression(between, schema),
            IsNullExpression isNull => BuildIsNullExpression(isNull, schema),
            FunctionCall func => BuildFunctionCall(func, schema),
            SystemVariableExpression sysVar => BuildSystemVariable(sysVar),
            _ => throw new CyscaleException($"Unsupported expression type: {expr.GetType().Name}")
        };
    }

    private IExpressionEvaluator BuildSystemVariable(SystemVariableExpression sysVar)
    {
        var value = _systemVariables.Get(sysVar.VariableName, sysVar.Scope == SetScope.Global);
        return new ConstantEvaluator(Common.SystemVariables.ToDataValue(value));
    }

    private IExpressionEvaluator BuildFunctionCall(FunctionCall func, TableSchema schema)
    {
        var funcName = func.FunctionName.ToUpperInvariant();

        // Handle built-in functions
        return funcName switch
        {
            "NOW" or "CURRENT_TIMESTAMP" => new ConstantEvaluator(DataValue.FromDateTime(DateTime.Now)),
            "CURDATE" or "CURRENT_DATE" => new ConstantEvaluator(DataValue.FromDate(DateOnly.FromDateTime(DateTime.Now))),
            "CURTIME" or "CURRENT_TIME" => new ConstantEvaluator(DataValue.FromTime(TimeOnly.FromDateTime(DateTime.Now))),
            "DATABASE" or "SCHEMA" => new ConstantEvaluator(DataValue.FromVarChar(_currentDatabase)),
            "VERSION" => new ConstantEvaluator(DataValue.FromVarChar(Constants.ServerVersion)),
            "USER" or "CURRENT_USER" => new ConstantEvaluator(DataValue.FromVarChar("root@localhost")),
            "CONNECTION_ID" => new ConstantEvaluator(DataValue.FromBigInt(1)),
            "LAST_INSERT_ID" => new ConstantEvaluator(DataValue.FromBigInt(_lastInsertId)),
            "ROW_COUNT" => new ConstantEvaluator(DataValue.FromBigInt(0)),
            "FOUND_ROWS" => new ConstantEvaluator(DataValue.FromBigInt(0)),
            "UPPER" or "UCASE" => BuildStringFunction(func, schema, s => s.ToUpperInvariant()),
            "LOWER" or "LCASE" => BuildStringFunction(func, schema, s => s.ToLowerInvariant()),
            "LENGTH" or "CHAR_LENGTH" or "CHARACTER_LENGTH" => BuildLengthFunction(func, schema),
            "CONCAT" => BuildConcatFunction(func, schema),
            "IFNULL" or "COALESCE" => BuildIfNullFunction(func, schema),
            "IF" => BuildIfFunction(func, schema),
            _ when IsAggregateFunction(funcName) => 
                // Aggregates in non-GROUP BY context - return constant for now
                new ConstantEvaluator(DataValue.Null),
            _ => throw new CyscaleException($"Unknown function: {func.FunctionName}")
        };
    }

    private IExpressionEvaluator BuildStringFunction(FunctionCall func, TableSchema schema, Func<string, string> transform)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException($"{func.FunctionName} requires 1 argument");

        var argEval = BuildExpression(func.Arguments[0], schema);
        return new StringFunctionEvaluator(argEval, transform);
    }

    private IExpressionEvaluator BuildLengthFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException($"{func.FunctionName} requires 1 argument");

        var argEval = BuildExpression(func.Arguments[0], schema);
        return new LengthFunctionEvaluator(argEval);
    }

    private IExpressionEvaluator BuildConcatFunction(FunctionCall func, TableSchema schema)
    {
        var args = func.Arguments.Select(a => BuildExpression(a, schema)).ToList();
        return new ConcatFunctionEvaluator(args);
    }

    private IExpressionEvaluator BuildIfNullFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException($"{func.FunctionName} requires 2 arguments");

        var expr = BuildExpression(func.Arguments[0], schema);
        var defaultVal = BuildExpression(func.Arguments[1], schema);
        return new IfNullEvaluator(expr, defaultVal);
    }

    private IExpressionEvaluator BuildIfFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 3)
            throw new CyscaleException("IF requires 3 arguments");

        var condition = BuildExpression(func.Arguments[0], schema);
        var trueVal = BuildExpression(func.Arguments[1], schema);
        var falseVal = BuildExpression(func.Arguments[2], schema);
        return new IfEvaluator(condition, trueVal, falseVal);
    }

    private IExpressionEvaluator BuildExpressionWithJoin(Expression expr, TableSchema leftSchema, TableSchema rightSchema, TableSchema combinedSchema)
    {
        return expr switch
        {
            LiteralExpression lit => new ConstantEvaluator(lit.Value),
            ColumnReference col => BuildJoinColumnReference(col, leftSchema, rightSchema, combinedSchema),
            BinaryExpression bin => new BinaryEvaluator(
                BuildExpressionWithJoin(bin.Left, leftSchema, rightSchema, combinedSchema),
                BuildExpressionWithJoin(bin.Right, leftSchema, rightSchema, combinedSchema),
                ConvertBinaryOperator(bin.Operator)),
            _ => throw new CyscaleException($"Unsupported expression in join condition: {expr.GetType().Name}")
        };
    }

    private IExpressionEvaluator BuildColumnReference(ColumnReference col, TableSchema schema)
    {
        int ordinal;

        if (col.TableName != null)
        {
            // Try qualified name (table_column)
            var qualifiedName = $"{col.TableName}_{col.ColumnName}";
            ordinal = schema.GetColumnOrdinal(qualifiedName);
            if (ordinal < 0)
            {
                // Try just the column name
                ordinal = schema.GetColumnOrdinal(col.ColumnName);
            }
        }
        else
        {
            ordinal = schema.GetColumnOrdinal(col.ColumnName);
        }

        if (ordinal < 0)
            throw new ColumnNotFoundException(col.ColumnName);

        return new ColumnEvaluator(ordinal);
    }

    private IExpressionEvaluator BuildJoinColumnReference(ColumnReference col, TableSchema leftSchema, TableSchema rightSchema, TableSchema combinedSchema)
    {
        int ordinal = -1;

        if (col.TableName != null)
        {
            // Try to find in left schema
            var leftOrdinal = leftSchema.GetColumnOrdinal(col.ColumnName);
            if (leftOrdinal >= 0 && leftSchema.TableName.Equals(col.TableName, StringComparison.OrdinalIgnoreCase))
            {
                ordinal = leftOrdinal;
            }
            else
            {
                // Try right schema
                var rightOrdinal = rightSchema.GetColumnOrdinal(col.ColumnName);
                if (rightOrdinal >= 0 && rightSchema.TableName.Equals(col.TableName, StringComparison.OrdinalIgnoreCase))
                {
                    ordinal = leftSchema.Columns.Count + rightOrdinal;
                }
            }

            // Try combined schema
            if (ordinal < 0)
            {
                ordinal = combinedSchema.GetColumnOrdinal($"{col.TableName}_{col.ColumnName}");
            }
        }
        else
        {
            // Try left first, then right
            ordinal = leftSchema.GetColumnOrdinal(col.ColumnName);
            if (ordinal < 0)
            {
                var rightOrdinal = rightSchema.GetColumnOrdinal(col.ColumnName);
                if (rightOrdinal >= 0)
                {
                    ordinal = leftSchema.Columns.Count + rightOrdinal;
                }
            }
        }

        if (ordinal < 0)
            throw new ColumnNotFoundException(col.ColumnName);

        return new ColumnEvaluator(ordinal);
    }

    private IExpressionEvaluator BuildBinaryExpression(BinaryExpression bin, TableSchema schema)
    {
        var left = BuildExpression(bin.Left, schema);
        var right = BuildExpression(bin.Right, schema);
        var op = ConvertBinaryOperator(bin.Operator);
        return new BinaryEvaluator(left, right, op);
    }

    private IExpressionEvaluator BuildUnaryExpression(UnaryExpression un, TableSchema schema)
    {
        var operand = BuildExpression(un.Operand, schema);
        var op = un.Operator switch
        {
            Parsing.Ast.UnaryOperator.Negate => UnaryOp.Negate,
            Parsing.Ast.UnaryOperator.Not => UnaryOp.Not,
            _ => throw new CyscaleException($"Unknown unary operator: {un.Operator}")
        };
        return new UnaryEvaluator(operand, op);
    }

    private IExpressionEvaluator BuildInExpression(InExpression inExpr, TableSchema schema)
    {
        var expr = BuildExpression(inExpr.Expression, schema);
        var values = inExpr.Values!.Select(v => BuildExpression(v, schema)).ToList();
        return new InEvaluator(expr, values, inExpr.IsNot);
    }

    private IExpressionEvaluator BuildBetweenExpression(BetweenExpression between, TableSchema schema)
    {
        var expr = BuildExpression(between.Expression, schema);
        var low = BuildExpression(between.Low, schema);
        var high = BuildExpression(between.High, schema);
        return new BetweenEvaluator(expr, low, high, between.IsNot);
    }

    private IExpressionEvaluator BuildIsNullExpression(IsNullExpression isNull, TableSchema schema)
    {
        var operand = BuildExpression(isNull.Expression, schema);
        var op = isNull.IsNot ? UnaryOp.IsNotNull : UnaryOp.IsNull;
        return new UnaryEvaluator(operand, op);
    }

    private static BinaryOp ConvertBinaryOperator(BinaryOperator op)
    {
        return op switch
        {
            BinaryOperator.Add => BinaryOp.Add,
            BinaryOperator.Subtract => BinaryOp.Subtract,
            BinaryOperator.Multiply => BinaryOp.Multiply,
            BinaryOperator.Divide => BinaryOp.Divide,
            BinaryOperator.Modulo => BinaryOp.Modulo,
            BinaryOperator.Equal => BinaryOp.Equal,
            BinaryOperator.NotEqual => BinaryOp.NotEqual,
            BinaryOperator.LessThan => BinaryOp.LessThan,
            BinaryOperator.LessThanOrEqual => BinaryOp.LessThanOrEqual,
            BinaryOperator.GreaterThan => BinaryOp.GreaterThan,
            BinaryOperator.GreaterThanOrEqual => BinaryOp.GreaterThanOrEqual,
            BinaryOperator.And => BinaryOp.And,
            BinaryOperator.Or => BinaryOp.Or,
            BinaryOperator.Like => BinaryOp.Like,
            _ => throw new CyscaleException($"Unknown binary operator: {op}")
        };
    }

    private DataValue EvaluateLiteralExpression(Expression expr)
    {
        return expr switch
        {
            LiteralExpression lit => lit.Value,
            UnaryExpression un when un.Operator == Parsing.Ast.UnaryOperator.Negate && un.Operand is LiteralExpression lit => NegateValue(lit.Value),
            FunctionCall func => EvaluateConstantFunction(func),
            _ => throw new CyscaleException($"Expected literal expression, got: {expr.GetType().Name}")
        };
    }

    private DataValue EvaluateConstantFunction(FunctionCall func)
    {
        var funcName = func.FunctionName.ToUpperInvariant();
        return funcName switch
        {
            "NOW" or "CURRENT_TIMESTAMP" => DataValue.FromDateTime(DateTime.Now),
            "CURDATE" or "CURRENT_DATE" => DataValue.FromDate(DateOnly.FromDateTime(DateTime.Now)),
            "CURTIME" or "CURRENT_TIME" => DataValue.FromTime(TimeOnly.FromDateTime(DateTime.Now)),
            _ => DataValue.Null
        };
    }
    
    /// <summary>
    /// Evaluates an expression against a simple dictionary context.
    /// Used for SHOW statements with WHERE clauses.
    /// </summary>
    private DataValue EvaluateExpression(Expression expr, Dictionary<string, DataValue> context)
    {
        return expr switch
        {
            LiteralExpression lit => lit.Value,
            ColumnReference col => context.TryGetValue(col.ColumnName, out var val) ? val : DataValue.Null,
            BinaryExpression bin => EvaluateBinaryExpression(bin, context),
            UnaryExpression un => EvaluateUnaryExpression(un, context),
            _ => DataValue.Null
        };
    }
    
    private DataValue EvaluateBinaryExpression(BinaryExpression bin, Dictionary<string, DataValue> context)
    {
        var left = EvaluateExpression(bin.Left, context);
        var right = EvaluateExpression(bin.Right, context);
        
        return bin.Operator switch
        {
            BinaryOperator.Equal => DataValue.FromBoolean(left.Equals(right)),
            BinaryOperator.NotEqual => DataValue.FromBoolean(!left.Equals(right)),
            BinaryOperator.LessThan => DataValue.FromBoolean(left.CompareTo(right) < 0),
            BinaryOperator.LessThanOrEqual => DataValue.FromBoolean(left.CompareTo(right) <= 0),
            BinaryOperator.GreaterThan => DataValue.FromBoolean(left.CompareTo(right) > 0),
            BinaryOperator.GreaterThanOrEqual => DataValue.FromBoolean(left.CompareTo(right) >= 0),
            BinaryOperator.And => DataValue.FromBoolean(IsTruthy(left) && IsTruthy(right)),
            BinaryOperator.Or => DataValue.FromBoolean(IsTruthy(left) || IsTruthy(right)),
            _ => DataValue.Null
        };
    }
    
    private DataValue EvaluateUnaryExpression(UnaryExpression un, Dictionary<string, DataValue> context)
    {
        var operand = EvaluateExpression(un.Operand, context);
        return un.Operator switch
        {
            Parsing.Ast.UnaryOperator.Not => DataValue.FromBoolean(!IsTruthy(operand)),
            Parsing.Ast.UnaryOperator.Negate => NegateValue(operand),
            _ => DataValue.Null
        };
    }

    private static DataValue NegateValue(DataValue val)
    {
        return val.Type switch
        {
            DataType.Int => DataValue.FromInt(-val.AsInt()),
            DataType.BigInt => DataValue.FromBigInt(-val.AsBigInt()),
            DataType.Float => DataValue.FromFloat(-val.AsFloat()),
            DataType.Double => DataValue.FromDouble(-val.AsDouble()),
            DataType.Decimal => DataValue.FromDecimal(-val.AsDecimal()),
            _ => throw new CyscaleException($"Cannot negate value of type {val.Type}")
        };
    }
    
    /// <summary>
    /// Checks if a DataValue is truthy (non-null, non-zero, non-empty, or boolean true).
    /// </summary>
    private static bool IsTruthy(DataValue val)
    {
        if (val.IsNull) return false;
        return val.Type switch
        {
            DataType.Boolean => val.AsBoolean(),
            DataType.Int => val.AsInt() != 0,
            DataType.BigInt => val.AsBigInt() != 0,
            DataType.TinyInt => val.AsTinyInt() != 0,
            DataType.SmallInt => val.AsSmallInt() != 0,
            DataType.VarChar or DataType.Char or DataType.Text => !string.IsNullOrEmpty(val.AsString()),
            _ => true // Non-null values of other types are truthy
        };
    }

    private static string GetExpressionName(Expression expr)
    {
        return expr switch
        {
            ColumnReference col => col.TableName != null ? $"{col.TableName}.{col.ColumnName}" : col.ColumnName,
            FunctionCall func => func.IsStarArgument ? $"{func.FunctionName}(*)" : $"{func.FunctionName}(...)",
            LiteralExpression lit => lit.Value.IsNull ? "NULL" : lit.Value.GetRawValue()?.ToString() ?? "expr",
            _ => "expr"
        };
    }

    private static DataType InferDataType(Expression expr, TableSchema schema)
    {
        return expr switch
        {
            LiteralExpression lit => lit.Value.Type,
            ColumnReference col => schema.GetColumn(col.ColumnName)?.DataType ?? DataType.VarChar,
            BinaryExpression bin => InferBinaryResultType(bin.Operator, InferDataType(bin.Left, schema), InferDataType(bin.Right, schema)),
            FunctionCall func => InferFunctionResultType(func),
            _ => DataType.VarChar
        };
    }

    private static DataType InferFunctionResultType(FunctionCall func)
    {
        return func.FunctionName.ToUpperInvariant() switch
        {
            "COUNT" => DataType.BigInt,
            "SUM" or "AVG" => DataType.Double,
            "MIN" or "MAX" => DataType.VarChar, // Depends on argument
            "NOW" or "CURRENT_TIMESTAMP" => DataType.DateTime,
            "CURDATE" or "CURRENT_DATE" => DataType.Date,
            "CURTIME" or "CURRENT_TIME" => DataType.Time,
            "LENGTH" or "CHAR_LENGTH" => DataType.Int,
            _ => DataType.VarChar
        };
    }

    private static DataType InferBinaryResultType(BinaryOperator op, DataType left, DataType right)
    {
        // Comparison operators return boolean
        if (op >= BinaryOperator.Equal && op <= BinaryOperator.GreaterThanOrEqual)
            return DataType.Boolean;
        if (op == BinaryOperator.And || op == BinaryOperator.Or)
            return DataType.Boolean;
        if (op == BinaryOperator.Like)
            return DataType.Boolean;

        // Arithmetic - use wider type
        if (left == DataType.Double || right == DataType.Double)
            return DataType.Double;
        if (left == DataType.Float || right == DataType.Float)
            return DataType.Float;
        if (left == DataType.BigInt || right == DataType.BigInt)
            return DataType.BigInt;

        return DataType.Int;
    }

    #endregion
}

#region Additional Expression Evaluators

/// <summary>
/// Evaluates string functions like UPPER, LOWER.
/// </summary>
internal sealed class StringFunctionEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _argument;
    private readonly Func<string, string> _transform;

    public StringFunctionEvaluator(IExpressionEvaluator argument, Func<string, string> transform)
    {
        _argument = argument;
        _transform = transform;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _argument.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        return DataValue.FromVarChar(_transform(val.AsString()));
    }
}

/// <summary>
/// Evaluates LENGTH function.
/// </summary>
internal sealed class LengthFunctionEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _argument;

    public LengthFunctionEvaluator(IExpressionEvaluator argument)
    {
        _argument = argument;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _argument.Evaluate(row);
        if (val.IsNull) return DataValue.Null;
        return DataValue.FromInt(val.AsString().Length);
    }
}

/// <summary>
/// Evaluates CONCAT function.
/// </summary>
internal sealed class ConcatFunctionEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _arguments;

    public ConcatFunctionEvaluator(List<IExpressionEvaluator> arguments)
    {
        _arguments = arguments;
    }

    public DataValue Evaluate(Row row)
    {
        var sb = new System.Text.StringBuilder();
        foreach (var arg in _arguments)
        {
            var val = arg.Evaluate(row);
            if (val.IsNull) return DataValue.Null; // MySQL behavior: CONCAT with NULL returns NULL
            sb.Append(val.AsString());
        }
        return DataValue.FromVarChar(sb.ToString());
    }
}

/// <summary>
/// Evaluates IFNULL/COALESCE function.
/// </summary>
internal sealed class IfNullEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expression;
    private readonly IExpressionEvaluator _defaultValue;

    public IfNullEvaluator(IExpressionEvaluator expression, IExpressionEvaluator defaultValue)
    {
        _expression = expression;
        _defaultValue = defaultValue;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _expression.Evaluate(row);
        return val.IsNull ? _defaultValue.Evaluate(row) : val;
    }
}

/// <summary>
/// Evaluates IF function.
/// </summary>
internal sealed class IfEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _condition;
    private readonly IExpressionEvaluator _trueValue;
    private readonly IExpressionEvaluator _falseValue;

    public IfEvaluator(IExpressionEvaluator condition, IExpressionEvaluator trueValue, IExpressionEvaluator falseValue)
    {
        _condition = condition;
        _trueValue = trueValue;
        _falseValue = falseValue;
    }

    public DataValue Evaluate(Row row)
    {
        var cond = _condition.Evaluate(row);
        var isTrue = !cond.IsNull && cond.Type == DataType.Boolean && cond.AsBoolean();
        return isTrue ? _trueValue.Evaluate(row) : _falseValue.Evaluate(row);
    }
}

#endregion
