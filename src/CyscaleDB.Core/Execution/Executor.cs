using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Execution.Operators;
using CyscaleDB.Core.Parsing;
using CyscaleDB.Core.Parsing.Ast;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.Execution;

/// <summary>
/// Executes SQL statements against the database.
/// </summary>
public sealed class Executor
{
    private readonly Catalog _catalog;
    private readonly Logger _logger;
    private string _currentDatabase;

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

    public Executor(Catalog catalog, string? defaultDatabase = null)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _logger = LogManager.Default.GetLogger<Executor>();
        _currentDatabase = defaultDatabase ?? Constants.DefaultDatabaseName;
    }

    /// <summary>
    /// Executes a SQL string.
    /// </summary>
    public ExecutionResult Execute(string sql)
    {
        var parser = new Parser(sql);
        var statement = parser.Parse();
        return Execute(statement);
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

        // FROM clause
        if (stmt.From != null)
        {
            op = BuildTableReference(stmt.From);
        }
        else
        {
            // SELECT without FROM (e.g., SELECT 1)
            throw new CyscaleException("SELECT without FROM is not yet supported");
        }

        // WHERE clause
        if (stmt.Where != null)
        {
            var predicate = BuildExpression(stmt.Where, op.Schema);
            op = new FilterOperator(op, predicate);
        }

        // Projection (SELECT columns)
        if (!IsSelectAll(stmt))
        {
            var projections = BuildProjections(stmt.Columns, op.Schema);
            op = new ProjectOperator(op, projections, _currentDatabase, "result");
        }

        // LIMIT/OFFSET
        if (stmt.Limit.HasValue)
        {
            op = new LimitOperator(op, stmt.Limit.Value, stmt.Offset ?? 0);
        }

        return op;
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
        var table = _catalog.GetTable(dbName, tableRef.TableName);
        
        if (table == null)
            throw new TableNotFoundException(tableRef.TableName);

        return new TableScanOperator(table, tableRef.Alias);
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
                }
            }

            var row = new Row(schema, values);
            table.InsertRow(row);
            insertedCount++;
        }

        table.Flush();
        _catalog.UpdateTableSchema(schema);

        _logger.Debug("Inserted {0} rows into {1}", insertedCount, stmt.TableName);
        return ExecutionResult.Modification(insertedCount);
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

    #region Utility Execution

    private ExecutionResult ExecuteShowTables(ShowTablesStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var tables = _catalog.ListTables(dbName);

        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = $"Tables_in_{dbName}", DataType = DataType.VarChar });

        foreach (var tableName in tables)
        {
            result.Rows.Add([DataValue.FromVarChar(tableName)]);
        }

        return ExecutionResult.Query(result);
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
        // TODO: Implement transaction support
        _logger.Debug("BEGIN transaction (not yet implemented)");
        return ExecutionResult.Ddl("Transaction started");
    }

    private ExecutionResult ExecuteCommit()
    {
        // TODO: Implement transaction support
        _catalog.Flush();
        _logger.Debug("COMMIT transaction (not yet implemented)");
        return ExecutionResult.Ddl("Transaction committed");
    }

    private ExecutionResult ExecuteRollback()
    {
        // TODO: Implement transaction support
        _logger.Debug("ROLLBACK transaction (not yet implemented)");
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
            _ => throw new CyscaleException($"Unsupported expression type: {expr.GetType().Name}")
        };
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
            _ => throw new CyscaleException($"Expected literal expression, got: {expr.GetType().Name}")
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

    private static string GetExpressionName(Expression expr)
    {
        return expr switch
        {
            ColumnReference col => col.TableName != null ? $"{col.TableName}.{col.ColumnName}" : col.ColumnName,
            FunctionCall func => $"{func.FunctionName}(...)",
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
