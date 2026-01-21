using CyscaleDB.Core.Auth;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Expressions;
using CyscaleDB.Core.Execution.Operators;
using CyscaleDB.Core.Parsing;
using CyscaleDB.Core.Parsing.Ast;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Index;
using CyscaleDB.Core.Storage.InformationSchema;
using CyscaleDB.Core.Storage.OnlineDdl;
using CyscaleDB.Core.Transactions;

namespace CyscaleDB.Core.Execution;

/// <summary>
/// Executes SQL statements against the database.
/// </summary>
public sealed class Executor
{
    private readonly Catalog _catalog;
    private readonly TransactionManager? _transactionManager;
    private readonly RecordLockManager _recordLockManager;
    private readonly ForeignKeyManager _foreignKeyManager;
    private readonly OnlineDdlManager _onlineDdlManager;
    private readonly Logger _logger;
    private string _currentDatabase;
    private Transaction? _currentTransaction;
    private long _lastInsertId;
    private readonly SystemVariables _systemVariables = new();

    // Current session user info
    private string _currentUser = "root";
    private string _currentHost = "localhost";
    
    // Locking context for current SELECT statement
    private LockingOptions? _currentLockingOptions;

    // CTE context for the current query execution
    private Dictionary<string, ResultSet>? _cteResults;

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
        : this(catalog, null, null, defaultDatabase)
    {
    }

    public Executor(Catalog catalog, TransactionManager? transactionManager, string? defaultDatabase = null)
        : this(catalog, transactionManager, null, defaultDatabase)
    {
    }

    public Executor(Catalog catalog, TransactionManager? transactionManager, ForeignKeyManager? foreignKeyManager, string? defaultDatabase = null)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _transactionManager = transactionManager;
        _foreignKeyManager = foreignKeyManager ?? new ForeignKeyManager();
        _onlineDdlManager = new OnlineDdlManager();
        _recordLockManager = new RecordLockManager();
        _logger = LogManager.Default.GetLogger<Executor>();
        _currentDatabase = defaultDatabase ?? Constants.DefaultDatabaseName;
    }

    /// <summary>
    /// Gets the foreign key manager for this executor.
    /// </summary>
    public ForeignKeyManager ForeignKeyManager => _foreignKeyManager;

    /// <summary>
    /// Sets the current session user for privilege checking.
    /// </summary>
    public void SetSessionUser(string username, string host = "localhost")
    {
        _currentUser = username;
        _currentHost = host;
    }

    /// <summary>
    /// Checks if the current user has the required privilege.
    /// Throws CyscaleException if the privilege is not granted.
    /// </summary>
    private void CheckPrivilege(PrivilegeType privilege, string? database = null, string? table = null)
    {
        if (!UserManager.Instance.HasPrivilege(_currentUser, _currentHost, privilege, database, table))
        {
            throw new CyscaleException(
                $"Access denied for user '{_currentUser}'@'{_currentHost}' to {privilege} on {database ?? "*"}.{table ?? "*"}",
                ErrorCode.AccessDenied);
        }
    }

    /// <summary>
    /// Determines the required privilege for a statement and checks it.
    /// </summary>
    private void CheckStatementPrivilege(Statement statement)
    {
        var (privilege, database, table) = GetRequiredPrivilege(statement);
        if (privilege.HasValue)
        {
            CheckPrivilege(privilege.Value, database, table);
        }
    }

    /// <summary>
    /// Extracts the database name from a table reference.
    /// </summary>
    private static string? GetDatabaseFromTableRef(TableReference? tableRef)
    {
        return tableRef switch
        {
            SimpleTableReference simple => simple.DatabaseName,
            JoinTableReference joined => GetDatabaseFromTableRef(joined.Left),
            SubqueryTableReference => null,
            _ => null
        };
    }

    /// <summary>
    /// Extracts the table name from a table reference.
    /// </summary>
    private static string? GetTableNameFromTableRef(TableReference? tableRef)
    {
        return tableRef switch
        {
            SimpleTableReference simple => simple.TableName,
            JoinTableReference joined => GetTableNameFromTableRef(joined.Left),
            SubqueryTableReference => null,
            _ => null
        };
    }

    /// <summary>
    /// Determines the required privilege for executing a statement.
    /// </summary>
    private (PrivilegeType? privilege, string? database, string? table) GetRequiredPrivilege(Statement statement)
    {
        return statement switch
        {
            SelectStatement s => (PrivilegeType.Select, GetDatabaseFromTableRef(s.From) ?? _currentDatabase, GetTableNameFromTableRef(s.From)),
            InsertStatement s => (PrivilegeType.Insert, s.DatabaseName ?? _currentDatabase, s.TableName),
            UpdateStatement s => (PrivilegeType.Update, s.DatabaseName ?? _currentDatabase, s.TableName),
            DeleteStatement s => (PrivilegeType.Delete, s.DatabaseName ?? _currentDatabase, s.TableName),
            CreateTableStatement s => (PrivilegeType.Create, s.DatabaseName ?? _currentDatabase, null),
            CreateDatabaseStatement => (PrivilegeType.Create, null, null),
            CreateViewStatement s => (PrivilegeType.CreateView, s.DatabaseName ?? _currentDatabase, null),
            CreateIndexStatement s => (PrivilegeType.Index, s.DatabaseName ?? _currentDatabase, s.TableName),
            DropTableStatement s => (PrivilegeType.Drop, s.DatabaseName ?? _currentDatabase, s.TableName),
            DropDatabaseStatement => (PrivilegeType.Drop, null, null),
            DropViewStatement => (PrivilegeType.Drop, _currentDatabase, null),
            DropIndexStatement s => (PrivilegeType.Index, s.DatabaseName ?? _currentDatabase, null),
            AlterTableStatement s => (PrivilegeType.Alter, s.DatabaseName ?? _currentDatabase, s.TableName),
            CreateProcedureStatement => (PrivilegeType.CreateRoutine, _currentDatabase, null),
            DropProcedureStatement => (PrivilegeType.AlterRoutine, _currentDatabase, null),
            CallStatement => (PrivilegeType.Execute, _currentDatabase, null),
            CreateTriggerStatement s => (PrivilegeType.Trigger, _currentDatabase, s.TableName),
            DropTriggerStatement => (PrivilegeType.Trigger, _currentDatabase, null),
            CreateEventStatement => (PrivilegeType.Event, _currentDatabase, null),
            DropEventStatement => (PrivilegeType.Event, _currentDatabase, null),
            GrantStatement => (PrivilegeType.Grant, null, null),
            RevokeStatement => (PrivilegeType.Grant, null, null),
            // SHOW statements and others generally don't require specific privileges
            _ => (null, null, null)
        };
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
        // Check privileges before execution
        CheckStatementPrivilege(statement);

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
            AlterTableStatement s => ExecuteAlterTable(s),
            // New statement types for Navicat compatibility
            SetStatement s => ExecuteSet(s),
            SetTransactionStatement s => ExecuteSetTransaction(s),
            ShowVariablesStatement s => ExecuteShowVariables(s),
            ShowStatusStatement s => ExecuteShowStatus(s),
            // User management statements
            CreateUserStatement s => ExecuteCreateUser(s),
            AlterUserStatement s => ExecuteAlterUser(s),
            DropUserStatement s => ExecuteDropUser(s),
            GrantStatement s => ExecuteGrant(s),
            RevokeStatement s => ExecuteRevoke(s),
            ShowCreateTableStatement s => ExecuteShowCreateTable(s),
            ShowColumnsStatement s => ExecuteShowColumns(s),
            ShowTableStatusStatement s => ExecuteShowTableStatus(s),
            ShowIndexStatement s => ExecuteShowIndex(s),
            ShowWarningsStatement => ExecuteShowWarnings(),
            ShowErrorsStatement => ExecuteShowErrors(),
            ShowCollationStatement s => ExecuteShowCollation(s),
            ShowCharsetStatement s => ExecuteShowCharset(s),
            // Stored procedure/function statements
            CreateProcedureStatement s => ExecuteCreateProcedure(s),
            DropProcedureStatement s => ExecuteDropProcedure(s),
            CreateFunctionStatement s => ExecuteCreateFunction(s),
            DropFunctionStatement s => ExecuteDropFunction(s),
            CallStatement s => ExecuteCall(s),
            DeclareVariableStatement s => ExecuteDeclareVariable(s),
            IfStatement s => ExecuteIf(s),
            WhileStatement s => ExecuteWhile(s),
            RepeatStatement s => ExecuteRepeat(s),
            LoopStatement s => ExecuteLoop(s),
            LeaveStatement s => ExecuteLeave(s),
            IterateStatement s => ExecuteIterate(s),
            ReturnStatement s => ExecuteReturn(s),
            // Trigger statements
            CreateTriggerStatement s => ExecuteCreateTrigger(s),
            DropTriggerStatement s => ExecuteDropTrigger(s),
            // Event statements
            CreateEventStatement s => ExecuteCreateEvent(s),
            DropEventStatement s => ExecuteDropEvent(s),
            // Admin statements
            AnalyzeTableStatement s => ExecuteAnalyzeTable(s),
            FlushStatement s => ExecuteFlush(s),
            LockTablesStatement s => ExecuteLockTables(s),
            UnlockTablesStatement => ExecuteUnlockTables(),
            _ => throw new CyscaleException($"Unsupported statement type: {statement.GetType().Name}")
        };
    }

    #region DML Execution

    private ExecutionResult ExecuteSelect(SelectStatement stmt)
    {
        // Handle FOR UPDATE / FOR SHARE locking
        var lockMode = stmt.LockMode;
        _currentLockingOptions = null;
        
        if (lockMode != SelectLockMode.None)
        {
            // FOR UPDATE requires an active transaction
            if (_currentTransaction == null && _transactionManager != null)
            {
                // Start implicit transaction for locking queries
                _currentTransaction = _transactionManager.Begin();
            }

            if (_currentTransaction != null)
            {
                // Create locking options for operators to use
                _currentLockingOptions = new LockingOptions(
                    lockMode,
                    _currentTransaction.TransactionId,
                    _currentDatabase,
                    _recordLockManager,
                    stmt.NoWait,
                    stmt.SkipLocked);
                
                // Apply locking semantics
                if (lockMode == SelectLockMode.ForUpdate)
                {
                    _logger.Debug("Executing SELECT FOR UPDATE with transaction {0}", _currentTransaction.TransactionId);
                }
                else if (lockMode == SelectLockMode.ForShare)
                {
                    _logger.Debug("Executing SELECT FOR SHARE with transaction {0}", _currentTransaction.TransactionId);
                }

                // NoWait option
                if (stmt.NoWait)
                {
                    _logger.Debug("NOWAIT option specified - will fail immediately if rows are locked");
                }

                // Skip Locked option
                if (stmt.SkipLocked)
                {
                    _logger.Debug("SKIP LOCKED option specified - will skip locked rows");
                }
            }
            else
            {
                _logger.Warning("FOR UPDATE/FOR SHARE without active transaction - locking will not be effective");
            }
        }

        try
        {
            var op = BuildSelectOperator(stmt);
            var resultSet = ResultSet.FromOperator(op);
            op.Dispose();
            return ExecutionResult.Query(resultSet);
        }
        finally
        {
            _currentLockingOptions = null;
            // Clean up CTE context after query execution
            _cteResults?.Clear();
            _cteResults = null;
        }
    }

    private IOperator BuildSelectOperator(SelectStatement stmt)
    {
        // Handle CTEs if present
        if (stmt.WithClause != null)
        {
            MaterializeCtes(stmt.WithClause);
        }

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

        // Set operations (UNION, INTERSECT, EXCEPT)
        if (stmt.SetOperationQueries.Count > 0)
        {
            op = BuildSetOperations(op, stmt);
        }
        else if (stmt.UnionQueries.Count > 0)
        {
            // Legacy UNION support
            op = BuildLegacyUnions(op, stmt);
        }

        return op;
    }

    /// <summary>
    /// Builds operators for set operations (UNION, INTERSECT, EXCEPT).
    /// </summary>
    private IOperator BuildSetOperations(IOperator baseOp, SelectStatement stmt)
    {
        var currentOp = baseOp;

        for (int i = 0; i < stmt.SetOperationQueries.Count; i++)
        {
            var rightQuery = stmt.SetOperationQueries[i];
            var opType = stmt.SetOperationTypes[i];
            var hasAll = stmt.SetOperationAllFlags[i];

            var rightOp = BuildSelectOperator(rightQuery);

            // Apply the set operation
            currentOp = opType switch
            {
                SetOperationType.Union => BuildUnionOperator(currentOp, rightOp, hasAll),
                SetOperationType.Intersect => BuildIntersectOperator(currentOp, rightOp, hasAll),
                SetOperationType.Except => BuildExceptOperator(currentOp, rightOp, hasAll),
                _ => throw new CyscaleException($"Unknown set operation type: {opType}")
            };
        }

        return currentOp;
    }

    /// <summary>
    /// Builds UNION operator (legacy support).
    /// </summary>
    private IOperator BuildLegacyUnions(IOperator baseOp, SelectStatement stmt)
    {
        var currentOp = baseOp;

        for (int i = 0; i < stmt.UnionQueries.Count; i++)
        {
            var rightQuery = stmt.UnionQueries[i];
            var hasAll = i < stmt.UnionAllFlags.Count && stmt.UnionAllFlags[i];

            var rightOp = BuildSelectOperator(rightQuery);
            currentOp = BuildUnionOperator(currentOp, rightOp, hasAll);
        }

        return currentOp;
    }

    /// <summary>
    /// Builds a UNION operator.
    /// </summary>
    private IOperator BuildUnionOperator(IOperator left, IOperator right, bool includeAll)
    {
        return new UnionOperator(left, right, includeAll);
    }

    /// <summary>
    /// Builds an INTERSECT operator.
    /// </summary>
    private IOperator BuildIntersectOperator(IOperator left, IOperator right, bool includeAll)
    {
        return new IntersectOperator(left, right, includeAll);
    }

    /// <summary>
    /// Builds an EXCEPT operator.
    /// </summary>
    private IOperator BuildExceptOperator(IOperator left, IOperator right, bool includeAll)
    {
        return new ExceptOperator(left, right, includeAll);
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
            "COUNT" or "SUM" or "AVG" or "MIN" or "MAX" or "GROUP_CONCAT" => true,
            _ => false
        };
    }

    private IOperator BuildGroupByOperator(SelectStatement stmt, IOperator input)
    {
        // Build group by key specs with proper names from SELECT columns
        var groupByKeys = new List<GroupByKeySpec>();
        var selectColIndex = 0;
        
        // First, find which SELECT columns correspond to GROUP BY keys
        // They appear in the same order as in the SELECT list (non-aggregate columns first)
        var nonAggSelectCols = stmt.Columns
            .Where(c => !(c.Expression is FunctionCall func && IsAggregateFunction(func.FunctionName)))
            .ToList();

        foreach (var expr in stmt.GroupBy)
        {
            var evaluator = BuildExpression(expr, input.Schema);
            
            // Try to find a matching SELECT column to get the output name
            string outputName;
            DataType outputType;
            
            if (selectColIndex < nonAggSelectCols.Count)
            {
                var selectCol = nonAggSelectCols[selectColIndex];
                outputName = selectCol.Alias ?? GetExpressionName(selectCol.Expression);
                outputType = InferDataType(selectCol.Expression, input.Schema);
                selectColIndex++;
            }
            else
            {
                // No matching SELECT column - use expression name
                outputName = GetExpressionName(expr);
                outputType = InferDataType(expr, input.Schema);
            }
            
            groupByKeys.Add(new GroupByKeySpec(evaluator, outputName, outputType));
        }

        // Build aggregate specifications from SELECT columns
        var aggregates = new List<AggregateSpec>();

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
                    "GROUP_CONCAT" => AggregateType.GroupConcat,
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
                    AggregateType.GroupConcat => DataType.Text,
                    _ => func.Arguments.Count > 0 ? InferDataType(func.Arguments[0], input.Schema) : DataType.BigInt
                };

                var name = col.Alias ?? $"{func.FunctionName}(*)";
                
                // Extract separator for GROUP_CONCAT (default is comma)
                var separator = func.Separator ?? ",";
                var isDistinct = func.IsDistinct;
                
                aggregates.Add(new AggregateSpec(aggType, argExpr, name, outputType, separator, isDistinct));
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
            CteTableReference cte => BuildCteTableReference(cte),
            _ => throw new CyscaleException($"Unsupported table reference type: {tableRef.GetType().Name}")
        };
    }

    /// <summary>
    /// Materializes CTEs by executing them and storing results.
    /// For recursive CTEs, iterates until no new rows are produced.
    /// </summary>
    private void MaterializeCtes(WithClause withClause)
    {
        _cteResults ??= new Dictionary<string, ResultSet>(StringComparer.OrdinalIgnoreCase);

        foreach (var cte in withClause.Ctes)
        {
            if (withClause.IsRecursive && IsRecursiveCte(cte))
            {
                MaterializeRecursiveCte(cte);
            }
            else
            {
                // Execute the non-recursive CTE query
                var cteOp = BuildSelectOperator(cte.Query);
                var resultSet = ResultSet.FromOperator(cteOp);
                cteOp.Dispose();

                // Store the result for later reference
                _cteResults[cte.Name] = resultSet;
                _logger.Debug("Materialized CTE '{0}' with {1} rows", cte.Name, resultSet.RowCount);
            }
        }
    }

    /// <summary>
    /// Checks if a CTE definition is recursive (references itself in the query).
    /// </summary>
    private static bool IsRecursiveCte(CteDefinition cte)
    {
        return ContainsCteReference(cte.Query, cte.Name);
    }

    /// <summary>
    /// Checks if a SELECT statement references a CTE by name.
    /// </summary>
    private static bool ContainsCteReference(SelectStatement stmt, string cteName)
    {
        // Check FROM clause
        if (stmt.From != null && ContainsCteReferenceInTableRef(stmt.From, cteName))
            return true;

        return false;
    }

    /// <summary>
    /// Checks if a table reference references a CTE by name.
    /// </summary>
    private static bool ContainsCteReferenceInTableRef(TableReference tableRef, string cteName)
    {
        if (tableRef is CteTableReference cteRef && 
            string.Equals(cteRef.CteName, cteName, StringComparison.OrdinalIgnoreCase))
            return true;

        if (tableRef is JoinTableReference join)
        {
            return ContainsCteReferenceInTableRef(join.Left, cteName) ||
                   ContainsCteReferenceInTableRef(join.Right, cteName);
        }

        if (tableRef is SimpleTableReference simple && 
            string.Equals(simple.TableName, cteName, StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
    }

    /// <summary>
    /// Materializes a recursive CTE by iterating until no new rows are produced.
    /// Recursive CTE structure: anchor_part UNION ALL recursive_part
    /// </summary>
    private void MaterializeRecursiveCte(CteDefinition cte)
    {
        const int MaxIterations = 1000; // Safety limit to prevent infinite recursion
        
        // Split the query into anchor and recursive parts
        // Typically, a recursive CTE has UNION ALL between anchor and recursive part
        var query = cte.Query;
        
        // Initialize with empty result set for self-reference
        var emptyResult = CreateEmptyCteResultSet(cte, query);
        _cteResults ??= new Dictionary<string, ResultSet>(StringComparer.OrdinalIgnoreCase);
        _cteResults[cte.Name] = emptyResult;

        // Execute the query iteratively
        var allRows = new List<DataValue[]>();
        int iteration = 0;
        int previousRowCount = 0;

        while (iteration < MaxIterations)
        {
            // Execute the CTE query (which may reference itself)
            var cteOp = BuildSelectOperator(query);
            var iterationResult = ResultSet.FromOperator(cteOp);
            cteOp.Dispose();

            // Collect new rows
            int newRows = 0;
            foreach (var row in iterationResult.Rows)
            {
                // Only add rows we haven't seen before
                if (!RowExistsInList(row, allRows))
                {
                    allRows.Add(row);
                    newRows++;
                }
            }

            iteration++;
            _logger.Trace("Recursive CTE '{0}' iteration {1}: {2} new rows", 
                cte.Name, iteration, newRows);

            // Update the CTE result for the next iteration
            var updatedResult = CreateEmptyCteResultSet(cte, query);
            foreach (var row in allRows)
            {
                updatedResult.Rows.Add(row);
            }
            _cteResults[cte.Name] = updatedResult;

            // Stop if no new rows were produced
            if (allRows.Count == previousRowCount)
                break;

            previousRowCount = allRows.Count;
        }

        if (iteration >= MaxIterations)
        {
            _logger.Warning("Recursive CTE '{0}' hit maximum iteration limit ({1})", 
                cte.Name, MaxIterations);
        }

        _logger.Debug("Materialized recursive CTE '{0}' with {1} rows in {2} iterations", 
            cte.Name, allRows.Count, iteration);
    }

    /// <summary>
    /// Creates an empty result set for a CTE with the proper columns.
    /// </summary>
    private static ResultSet CreateEmptyCteResultSet(CteDefinition cte, SelectStatement query)
    {
        var result = new ResultSet();
        
        // If columns are explicitly specified in the CTE definition, use those
        if (cte.Columns.Count > 0)
        {
            foreach (var colName in cte.Columns)
            {
                result.Columns.Add(new ResultColumn { Name = colName, DataType = DataType.VarChar });
            }
        }
        else
        {
            // Otherwise, derive from the query's SELECT columns
            foreach (var col in query.Columns)
            {
                var name = col.Alias ?? (col.Expression as ColumnReference)?.ColumnName ?? "column";
                result.Columns.Add(new ResultColumn { Name = name, DataType = DataType.VarChar });
            }
        }

        return result;
    }

    /// <summary>
    /// Checks if a row already exists in a list of rows.
    /// </summary>
    private static bool RowExistsInList(DataValue[] row, List<DataValue[]> rows)
    {
        foreach (var existingRow in rows)
        {
            if (existingRow.Length != row.Length)
                continue;

            bool matches = true;
            for (int i = 0; i < row.Length; i++)
            {
                if (!existingRow[i].Equals(row[i]))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Builds an operator for a CTE reference.
    /// </summary>
    private IOperator BuildCteTableReference(CteTableReference cteRef)
    {
        if (_cteResults == null || !_cteResults.TryGetValue(cteRef.CteName, out var resultSet))
        {
            throw new CyscaleException($"CTE '{cteRef.CteName}' not found");
        }

        // Return an operator that reads from the CTE result
        return new CteOperator(resultSet, cteRef.Alias ?? cteRef.CteName);
    }

    private IOperator BuildSimpleTableReference(SimpleTableReference tableRef)
    {
        var dbName = tableRef.DatabaseName ?? _currentDatabase;

        // Check if this is a CTE reference (no database qualifier and matches a CTE name)
        if (tableRef.DatabaseName == null && _cteResults != null && 
            _cteResults.TryGetValue(tableRef.TableName, out var cteResult))
        {
            return new CteOperator(cteResult, tableRef.Alias ?? tableRef.TableName);
        }

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

        // Create TableScanOperator with MVCC and locking support
        Storage.Mvcc.ReadView? readView = null;
        Storage.Mvcc.VersionChainManager? versionChainManager = null;
        
        // Get ReadView for MVCC if in transaction
        if (_currentTransaction != null && _transactionManager != null)
        {
            readView = _transactionManager.GetOrCreateReadView(_currentTransaction);
            versionChainManager = new Storage.Mvcc.VersionChainManager(_transactionManager.UndoLog);
        }

        return new TableScanOperator(table, tableRef.Alias, readView, versionChainManager, _currentLockingOptions);
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

        IExpressionEvaluator? condition = null;

        // Handle different join conditions
        if (join.IsNatural)
        {
            // NATURAL JOIN: implicit equality on all columns with same names
            condition = BuildNaturalJoinCondition(left.Schema, right.Schema, combinedSchema);
        }
        else if (join.UsingColumns.Count > 0)
        {
            // USING clause: equality on specified columns
            condition = BuildUsingJoinCondition(join.UsingColumns, left.Schema, right.Schema, combinedSchema);
        }
        else if (join.Condition != null)
        {
            // ON clause: explicit condition
            condition = BuildJoinCondition(join.Condition, left.Schema, right.Schema, combinedSchema);
        }

        var joinType = join.JoinType switch
        {
            Parsing.Ast.JoinType.Inner => JoinOperatorType.Inner,
            Parsing.Ast.JoinType.Left => JoinOperatorType.Left,
            Parsing.Ast.JoinType.Right => JoinOperatorType.Right,
            Parsing.Ast.JoinType.Full => JoinOperatorType.Full,
            Parsing.Ast.JoinType.Cross => JoinOperatorType.Cross,
            Parsing.Ast.JoinType.Natural => JoinOperatorType.Inner, // NATURAL JOIN is a type of inner join
            _ => JoinOperatorType.Inner
        };

        return new NestedLoopJoinOperator(left, right, condition, joinType);
    }

    /// <summary>
    /// Builds join condition for NATURAL JOIN (equality on all common columns).
    /// </summary>
    private IExpressionEvaluator BuildNaturalJoinCondition(TableSchema leftSchema, TableSchema rightSchema, TableSchema combinedSchema)
    {
        var commonColumns = new List<string>();

        // Find columns that exist in both schemas
        foreach (var leftCol in leftSchema.Columns)
        {
            foreach (var rightCol in rightSchema.Columns)
            {
                if (leftCol.Name.Equals(rightCol.Name, StringComparison.OrdinalIgnoreCase))
                {
                    commonColumns.Add(leftCol.Name);
                    break;
                }
            }
        }

        if (commonColumns.Count == 0)
        {
            // No common columns - same as CROSS JOIN
            return new ConstantEvaluator(DataValue.FromBoolean(true));
        }

        // Build equality conditions for all common columns
        return BuildUsingJoinCondition(commonColumns, leftSchema, rightSchema, combinedSchema);
    }

    /// <summary>
    /// Builds join condition for USING clause (equality on specified columns).
    /// </summary>
    private IExpressionEvaluator BuildUsingJoinCondition(
        List<string> columns, 
        TableSchema leftSchema, 
        TableSchema rightSchema, 
        TableSchema combinedSchema)
    {
        if (columns.Count == 0)
        {
            return new ConstantEvaluator(DataValue.FromBoolean(true));
        }

        IExpressionEvaluator? condition = null;

        foreach (var columnName in columns)
        {
            // Find column in left and right schemas
            var leftOrdinal = leftSchema.GetColumnOrdinal(columnName);
            var rightOrdinal = rightSchema.GetColumnOrdinal(columnName);

            if (leftOrdinal < 0 || rightOrdinal < 0)
            {
                throw new ColumnNotFoundException(columnName, "join");
            }

            // Build equality condition: left.column = right.column
            var leftColEvaluator = new ColumnEvaluator(leftOrdinal);
            var rightColEvaluator = new ColumnEvaluator(leftSchema.Columns.Count + rightOrdinal);
            var equalityCondition = new BinaryEvaluator(leftColEvaluator, rightColEvaluator, BinaryOp.Equal);

            // Combine with AND if there are multiple columns
            if (condition == null)
            {
                condition = equalityCondition;
            }
            else
            {
                condition = new BinaryEvaluator(condition, equalityCondition, BinaryOp.And);
            }
        }

        return condition ?? new ConstantEvaluator(DataValue.FromBoolean(true));
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

            // Execute BEFORE INSERT triggers
            ExecuteTriggers(stmt.TableName, TriggerTiming.Before, TriggerEvent.Insert, null, row);

            // Validate foreign key constraints on the new row
            _foreignKeyManager.ValidateInsert(
                dbName,
                stmt.TableName,
                row,
                schema,
                ReferencedRowExists);

            // Validate CHECK constraints on the new row
            ValidateCheckConstraints(dbName, stmt.TableName, row, schema);

            table.InsertRow(row);
            insertedCount++;

            // Execute AFTER INSERT triggers
            ExecuteTriggers(stmt.TableName, TriggerTiming.After, TriggerEvent.Insert, null, row);
        }

        table.Flush();
        _catalog.UpdateTableSchema(schema);
        _lastInsertId = lastId;

        _logger.Debug("Inserted {0} rows into {1}", insertedCount, stmt.TableName);
        return ExecutionResult.Modification(insertedCount, lastId);
    }

    /// <summary>
    /// Validates all CHECK constraints on a row.
    /// Throws an exception if any constraint is violated.
    /// </summary>
    private void ValidateCheckConstraints(string dbName, string tableName, Row row, TableSchema schema)
    {
        var db = _catalog.GetDatabase(dbName);
        if (db == null) return;

        var checkConstraints = db.GetCheckConstraintsOnTable(tableName);
        if (checkConstraints.Count == 0) return;

        foreach (var constraint in checkConstraints)
        {
            // Get or parse the expression
            var expression = constraint.Expression;
            if (expression == null)
            {
                // Parse the expression from text (needed after deserialization)
                var parser = new Parsing.Parser(constraint.ExpressionText);
                expression = parser.ParseSingleExpression();
            }

            // Build evaluator and evaluate against the row
            var evaluator = BuildExpression(expression, schema);
            var result = evaluator.Evaluate(row);

            // CHECK constraint must evaluate to TRUE (not FALSE, not NULL)
            if (result.Type != DataType.Boolean || !result.AsBoolean())
            {
                throw new CyscaleException(
                    $"CHECK constraint '{constraint.ConstraintName}' violated: {constraint.ExpressionText}",
                    ErrorCode.ConstraintViolation);
            }
        }
    }

    /// <summary>
    /// Checks if a referenced row exists in the target table.
    /// Used for foreign key validation.
    /// </summary>
    private bool ReferencedRowExists(string dbName, string tableName, DataValue[] keyValues)
    {
        var refTable = _catalog.GetTable(dbName, tableName);
        if (refTable == null)
            return false;

        var refSchema = refTable.Schema;
        
        // Find primary key columns
        var pkColumns = refSchema.Columns.Where(c => c.IsPrimaryKey).ToList();
        if (pkColumns.Count == 0)
        {
            // If no PK defined, look for unique index columns
            // For simplicity, just check all rows
            return CheckRowExists(refTable, refSchema, keyValues);
        }

        // Try index lookup first (if clustered index exists)
        return CheckRowExists(refTable, refSchema, keyValues);
    }

    /// <summary>
    /// Checks if a row with the given key values exists.
    /// </summary>
    private static bool CheckRowExists(Table table, TableSchema schema, DataValue[] keyValues)
    {
        // Get primary key column ordinals
        var pkColumns = schema.Columns.Where(c => c.IsPrimaryKey).ToList();
        
        foreach (var row in table.ScanTable())
        {
            bool matches = true;
            
            if (pkColumns.Count > 0 && pkColumns.Count == keyValues.Length)
            {
                // Match against primary key
                for (int i = 0; i < pkColumns.Count; i++)
                {
                    var rowValue = row.Values[pkColumns[i].OrdinalPosition];
                    if (!rowValue.Equals(keyValues[i]))
                    {
                        matches = false;
                        break;
                    }
                }
            }
            else
            {
                // Generic match - check first N columns
                for (int i = 0; i < keyValues.Length && i < row.Values.Length; i++)
                {
                    if (!row.Values[i].Equals(keyValues[i]))
                    {
                        matches = false;
                        break;
                    }
                }
            }
            
            if (matches)
                return true;
        }
        
        return false;
    }

    /// <summary>
    /// Checks if any rows reference the given values in a child table.
    /// Used for foreign key validation on DELETE/UPDATE.
    /// </summary>
    private bool ReferencingRowsExist(string dbName, string tableName, IReadOnlyList<string> fkColumns, DataValue[] keyValues)
    {
        var childTable = _catalog.GetTable(dbName, tableName);
        if (childTable == null)
            return false;

        var childSchema = childTable.Schema;

        foreach (var row in childTable.ScanTable())
        {
            bool matches = true;
            for (int i = 0; i < fkColumns.Count; i++)
            {
                var colOrdinal = childSchema.GetColumnOrdinal(fkColumns[i]);
                if (colOrdinal < 0)
                {
                    matches = false;
                    break;
                }

                var rowValue = row.Values[colOrdinal];
                
                // NULL FK values don't match (NULL != anything)
                if (rowValue.IsNull)
                {
                    matches = false;
                    break;
                }

                if (!rowValue.Equals(keyValues[i]))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
                return true;
        }

        return false;
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

        // Get columns being updated
        var updatingColumns = stmt.SetClauses.Select(s => s.ColumnName).ToHashSet(StringComparer.OrdinalIgnoreCase);

        // Check if any referenced columns (from FK constraints referencing this table) are being updated
        var fksReferencingThisTable = _foreignKeyManager.GetForeignKeysReferencingTable(dbName, stmt.TableName);
        bool updatingReferencedColumns = false;
        foreach (var fk in fksReferencingThisTable)
        {
            foreach (var refCol in fk.ReferencedColumns)
            {
                if (updatingColumns.Contains(refCol))
                {
                    updatingReferencedColumns = true;
                    break;
                }
            }
            if (updatingReferencedColumns) break;
        }

        // Collect rows to update and their new values (validate FK before any updates)
        var rowsToUpdate = new List<(RowId RowId, Row OldRow, Row NewRow)>();

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

            // Validate CHECK constraints on the new row values
            ValidateCheckConstraints(dbName, stmt.TableName, newRow, schema);

            rowsToUpdate.Add((row.RowId, row, newRow));
        }

        // Phase 1: Validate FK constraints and collect cascade actions
        var allCascadeUpdates = new List<(string DbName, string TableName, ForeignKeyInfo Fk, DataValue[] OldParentKeyValues, DataValue[] NewParentKeyValues)>();
        var allCascadeSetNulls = new List<(string DbName, string TableName, ForeignKeyInfo Fk, DataValue[] ParentKeyValues)>();

        foreach (var (_, oldRow, newRow) in rowsToUpdate)
        {
            // Only check if this update would orphan child rows if we're updating referenced columns
            if (updatingReferencedColumns)
            {
                var cascadeActions = _foreignKeyManager.ValidateDeleteOrUpdate(
                    dbName,
                    stmt.TableName,
                    oldRow,
                    schema,
                    ReferencingRowsExist,
                    isDelete: false);

                // Collect cascade actions for later execution
                foreach (var (fk, action) in cascadeActions)
                {
                    // Get the old and new referenced column values from the parent row
                    var oldParentKeyValues = new DataValue[fk.ReferencedColumns.Count];
                    var newParentKeyValues = new DataValue[fk.ReferencedColumns.Count];
                    for (int i = 0; i < fk.ReferencedColumns.Count; i++)
                    {
                        var colOrdinal = schema.GetColumnOrdinal(fk.ReferencedColumns[i]);
                        oldParentKeyValues[i] = oldRow.Values[colOrdinal];
                        newParentKeyValues[i] = newRow.Values[colOrdinal];
                    }

                    if (action == ForeignKeyAction.Cascade)
                    {
                        allCascadeUpdates.Add((fk.DatabaseName, fk.TableName, fk, oldParentKeyValues, newParentKeyValues));
                    }
                    else if (action == ForeignKeyAction.SetNull)
                    {
                        allCascadeSetNulls.Add((fk.DatabaseName, fk.TableName, fk, oldParentKeyValues));
                    }
                }
            }

            // Validate that new FK values exist in referenced tables
            _foreignKeyManager.ValidateInsert(
                dbName,
                stmt.TableName,
                newRow,
                schema,
                ReferencedRowExists);
        }

        // Phase 2: Execute cascade SET NULL operations first
        foreach (var (childDbName, childTableName, fk, parentKeyValues) in allCascadeSetNulls)
        {
            ExecuteCascadeSetNull(childDbName, childTableName, fk, parentKeyValues);
        }

        // Phase 3: Execute cascade UPDATE operations
        foreach (var (childDbName, childTableName, fk, oldParentKeyValues, newParentKeyValues) in allCascadeUpdates)
        {
            ExecuteCascadeUpdate(childDbName, childTableName, fk, oldParentKeyValues, newParentKeyValues);
        }

        // Phase 4: Perform the actual updates on the parent table
        foreach (var (rowId, oldRow, newRow) in rowsToUpdate)
        {
            // Execute BEFORE UPDATE triggers
            ExecuteTriggers(stmt.TableName, TriggerTiming.Before, TriggerEvent.Update, oldRow, newRow);

            if (table.UpdateRow(rowId, newRow))
            {
                updatedCount++;

                // Execute AFTER UPDATE triggers
                ExecuteTriggers(stmt.TableName, TriggerTiming.After, TriggerEvent.Update, oldRow, newRow);
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

        // Collect rows to delete with their data (can't modify while iterating)
        var rowsToDelete = new List<(RowId RowId, Row Row)>();

        foreach (var row in table.ScanTable())
        {
            // Check predicate
            if (predicate != null)
            {
                var result = predicate.Evaluate(row);
                if (result.Type != DataType.Boolean || !result.AsBoolean())
                    continue;
            }

            rowsToDelete.Add((row.RowId, row));
        }

        // Phase 1: Validate FK constraints and collect cascade actions
        var allCascadeDeletes = new List<(string DbName, string TableName, ForeignKeyInfo Fk, DataValue[] ParentKeyValues)>();
        var allCascadeSetNulls = new List<(string DbName, string TableName, ForeignKeyInfo Fk, DataValue[] ParentKeyValues)>();

        foreach (var (_, row) in rowsToDelete)
        {
            // Check if this delete would orphan any child rows
            var cascadeActions = _foreignKeyManager.ValidateDeleteOrUpdate(
                dbName,
                stmt.TableName,
                row,
                schema,
                ReferencingRowsExist,
                isDelete: true);

            // Collect cascade actions for later execution
            foreach (var (fk, action) in cascadeActions)
            {
                // Get the referenced column values from the parent row
                var parentKeyValues = new DataValue[fk.ReferencedColumns.Count];
                for (int i = 0; i < fk.ReferencedColumns.Count; i++)
                {
                    var colOrdinal = schema.GetColumnOrdinal(fk.ReferencedColumns[i]);
                    parentKeyValues[i] = row.Values[colOrdinal];
                }

                if (action == ForeignKeyAction.Cascade)
                {
                    allCascadeDeletes.Add((fk.DatabaseName, fk.TableName, fk, parentKeyValues));
                }
                else if (action == ForeignKeyAction.SetNull)
                {
                    allCascadeSetNulls.Add((fk.DatabaseName, fk.TableName, fk, parentKeyValues));
                }
            }
        }

        // Phase 2: Execute cascade SET NULL operations first
        foreach (var (childDbName, childTableName, fk, parentKeyValues) in allCascadeSetNulls)
        {
            ExecuteCascadeSetNull(childDbName, childTableName, fk, parentKeyValues);
        }

        // Phase 3: Execute cascade DELETE operations
        foreach (var (childDbName, childTableName, fk, parentKeyValues) in allCascadeDeletes)
        {
            ExecuteCascadeDelete(childDbName, childTableName, fk, parentKeyValues);
        }

        // Phase 4: Perform the actual deletes on the parent table
        foreach (var (rowId, row) in rowsToDelete)
        {
            // Execute BEFORE DELETE triggers
            ExecuteTriggers(stmt.TableName, TriggerTiming.Before, TriggerEvent.Delete, row, null);

            if (table.DeleteRow(rowId))
            {
                deletedCount++;

                // Execute AFTER DELETE triggers
                ExecuteTriggers(stmt.TableName, TriggerTiming.After, TriggerEvent.Delete, row, null);
            }
        }

        table.Flush();

        _logger.Debug("Deleted {0} rows from {1}", deletedCount, stmt.TableName);
        return ExecutionResult.Modification(deletedCount);
    }

    /// <summary>
    /// Executes cascade DELETE on child table rows that reference the deleted parent.
    /// </summary>
    private void ExecuteCascadeDelete(string dbName, string tableName, ForeignKeyInfo fk, DataValue[] parentKeyValues)
    {
        var childTable = _catalog.GetTable(dbName, tableName);
        if (childTable == null)
            return;

        var childSchema = childTable.Schema;

        // Find and delete all child rows that match the parent key
        var rowsToDelete = new List<(RowId RowId, Row Row)>();

        foreach (var row in childTable.ScanTable())
        {
            bool matches = true;
            for (int i = 0; i < fk.Columns.Count; i++)
            {
                var colOrdinal = childSchema.GetColumnOrdinal(fk.Columns[i]);
                if (colOrdinal < 0)
                {
                    matches = false;
                    break;
                }

                var rowValue = row.Values[colOrdinal];
                if (rowValue.IsNull || !rowValue.Equals(parentKeyValues[i]))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                rowsToDelete.Add((row.RowId, row));
            }
        }

        // Before deleting, check for grandchildren (recursive cascade)
        foreach (var (_, row) in rowsToDelete)
        {
            var grandchildCascades = _foreignKeyManager.ValidateDeleteOrUpdate(
                dbName,
                tableName,
                row,
                childSchema,
                ReferencingRowsExist,
                isDelete: true);

            // Handle grandchildren cascades
            foreach (var (grandchildFk, action) in grandchildCascades)
            {
                var grandchildKeyValues = new DataValue[grandchildFk.ReferencedColumns.Count];
                for (int i = 0; i < grandchildFk.ReferencedColumns.Count; i++)
                {
                    var colOrdinal = childSchema.GetColumnOrdinal(grandchildFk.ReferencedColumns[i]);
                    grandchildKeyValues[i] = row.Values[colOrdinal];
                }

                if (action == ForeignKeyAction.Cascade)
                {
                    ExecuteCascadeDelete(grandchildFk.DatabaseName, grandchildFk.TableName, grandchildFk, grandchildKeyValues);
                }
                else if (action == ForeignKeyAction.SetNull)
                {
                    ExecuteCascadeSetNull(grandchildFk.DatabaseName, grandchildFk.TableName, grandchildFk, grandchildKeyValues);
                }
            }
        }

        // Now delete the child rows
        foreach (var (rowId, _) in rowsToDelete)
        {
            childTable.DeleteRow(rowId);
        }

        childTable.Flush();
        _logger.Debug("Cascade deleted {0} rows from {1}.{2}", rowsToDelete.Count, dbName, tableName);
    }

    /// <summary>
    /// Executes cascade UPDATE on child table rows when parent key values change.
    /// </summary>
    private void ExecuteCascadeUpdate(string dbName, string tableName, ForeignKeyInfo fk, DataValue[] oldParentKeyValues, DataValue[] newParentKeyValues)
    {
        var childTable = _catalog.GetTable(dbName, tableName);
        if (childTable == null)
            return;

        var childSchema = childTable.Schema;

        // Find and update all child rows that match the old parent key
        var rowsToUpdate = new List<(RowId RowId, Row Row)>();

        foreach (var row in childTable.ScanTable())
        {
            bool matches = true;
            for (int i = 0; i < fk.Columns.Count; i++)
            {
                var colOrdinal = childSchema.GetColumnOrdinal(fk.Columns[i]);
                if (colOrdinal < 0)
                {
                    matches = false;
                    break;
                }

                var rowValue = row.Values[colOrdinal];
                if (rowValue.IsNull || !rowValue.Equals(oldParentKeyValues[i]))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                rowsToUpdate.Add((row.RowId, row));
            }
        }

        // Update FK columns to new parent key values
        foreach (var (rowId, row) in rowsToUpdate)
        {
            var newRow = row.Clone();
            for (int i = 0; i < fk.Columns.Count; i++)
            {
                var colOrdinal = childSchema.GetColumnOrdinal(fk.Columns[i]);
                if (colOrdinal >= 0)
                {
                    newRow.Values[colOrdinal] = newParentKeyValues[i];
                }
            }

            // Before updating, check for grandchildren cascade
            var grandchildCascades = _foreignKeyManager.ValidateDeleteOrUpdate(
                dbName,
                tableName,
                row,
                childSchema,
                ReferencingRowsExist,
                isDelete: false);

            // Handle grandchildren cascades recursively
            foreach (var (grandchildFk, action) in grandchildCascades)
            {
                // Get old and new key values for the grandchild FK
                var oldGrandchildKeyValues = new DataValue[grandchildFk.ReferencedColumns.Count];
                var newGrandchildKeyValues = new DataValue[grandchildFk.ReferencedColumns.Count];
                for (int j = 0; j < grandchildFk.ReferencedColumns.Count; j++)
                {
                    var colOrdinal = childSchema.GetColumnOrdinal(grandchildFk.ReferencedColumns[j]);
                    oldGrandchildKeyValues[j] = row.Values[colOrdinal];
                    newGrandchildKeyValues[j] = newRow.Values[colOrdinal];
                }

                if (action == ForeignKeyAction.Cascade)
                {
                    ExecuteCascadeUpdate(grandchildFk.DatabaseName, grandchildFk.TableName, grandchildFk, oldGrandchildKeyValues, newGrandchildKeyValues);
                }
                else if (action == ForeignKeyAction.SetNull)
                {
                    ExecuteCascadeSetNull(grandchildFk.DatabaseName, grandchildFk.TableName, grandchildFk, oldGrandchildKeyValues);
                }
            }

            childTable.UpdateRow(rowId, newRow);
        }

        childTable.Flush();
        _logger.Debug("Cascade updated {0} rows in {1}.{2}", rowsToUpdate.Count, dbName, tableName);
    }

    /// <summary>
    /// Executes cascade SET NULL on child table rows that reference the deleted/updated parent.
    /// </summary>
    private void ExecuteCascadeSetNull(string dbName, string tableName, ForeignKeyInfo fk, DataValue[] parentKeyValues)
    {
        var childTable = _catalog.GetTable(dbName, tableName);
        if (childTable == null)
            return;

        var childSchema = childTable.Schema;

        // Find and update all child rows that match the parent key
        var rowsToUpdate = new List<(RowId RowId, Row Row)>();

        foreach (var row in childTable.ScanTable())
        {
            bool matches = true;
            for (int i = 0; i < fk.Columns.Count; i++)
            {
                var colOrdinal = childSchema.GetColumnOrdinal(fk.Columns[i]);
                if (colOrdinal < 0)
                {
                    matches = false;
                    break;
                }

                var rowValue = row.Values[colOrdinal];
                if (rowValue.IsNull || !rowValue.Equals(parentKeyValues[i]))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                rowsToUpdate.Add((row.RowId, row));
            }
        }

        // Set FK columns to NULL
        foreach (var (rowId, row) in rowsToUpdate)
        {
            var newRow = row.Clone();
            for (int i = 0; i < fk.Columns.Count; i++)
            {
                var colOrdinal = childSchema.GetColumnOrdinal(fk.Columns[i]);
                if (colOrdinal >= 0)
                {
                    newRow.Values[colOrdinal] = DataValue.Null;
                }
            }
            childTable.UpdateRow(rowId, newRow);
        }

        childTable.Flush();
        _logger.Debug("Cascade SET NULL on {0} rows in {1}.{2}", rowsToUpdate.Count, dbName, tableName);
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

        // Process table-level constraints
        var db = _catalog.GetDatabase(dbName);
        if (db != null && stmt.Constraints.Count > 0)
        {
            foreach (var constraint in stmt.Constraints)
            {
                switch (constraint.Type)
                {
                    case ConstraintType.ForeignKey:
                        var fkDef = ForeignKeyDefinition.FromConstraint(stmt.TableName, constraint);
                        db.AddForeignKey(fkDef);
                        _foreignKeyManager.AddForeignKey(
                            constraint.Name ?? fkDef.ConstraintName,
                            dbName,
                            stmt.TableName,
                            constraint.Columns,
                            dbName, // referenced database is same as current
                            constraint.ReferencedTable!,
                            constraint.ReferencedColumns,
                            ConvertReferentialAction(constraint.OnDelete),
                            ConvertReferentialAction(constraint.OnUpdate));
                        break;

                    case ConstraintType.Check:
                        if (constraint.CheckExpression != null)
                        {
                            var checkDef = CheckConstraintDefinition.FromConstraint(stmt.TableName, constraint);
                            db.AddCheckConstraint(checkDef);
                        }
                        break;
                }
            }
            _catalog.UpdateTableSchema(_catalog.GetTableSchema(dbName, stmt.TableName)!);
        }

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

    private ExecutionResult ExecuteAlterTable(AlterTableStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var schema = _catalog.GetTableSchema(dbName, stmt.TableName);
        
        if (schema == null)
            throw new TableNotFoundException(stmt.TableName);

        int actionsPerformed = 0;
        var messages = new List<string>();

        foreach (var action in stmt.Actions)
        {
            switch (action)
            {
                case AddColumnAction addCol:
                    // Online ADD COLUMN - adds to schema without blocking DML (instant DDL)
                    var newColDef = ConvertColumnDefToStorageDefinition(addCol.Column);
                    schema.AddColumn(newColDef, addCol.AfterColumn);
                    _catalog.Flush();
                    messages.Add($"Added column '{addCol.Column.Name}'");
                    actionsPerformed++;
                    break;

                case DropColumnAction dropCol:
                    if (schema.GetColumnOrdinal(dropCol.ColumnName) < 0)
                        throw new ColumnNotFoundException(dropCol.ColumnName, stmt.TableName);
                    
                    // Online DROP COLUMN - removes from schema (instant DDL)
                    schema.DropColumn(dropCol.ColumnName);
                    _catalog.Flush();
                    messages.Add($"Dropped column '{dropCol.ColumnName}'");
                    actionsPerformed++;
                    break;

                case ModifyColumnAction modCol:
                    if (schema.GetColumnOrdinal(modCol.Column.Name) < 0)
                        throw new ColumnNotFoundException(modCol.Column.Name, stmt.TableName);
                    
                    var modColDef = ConvertColumnDefToStorageDefinition(modCol.Column);
                    schema.ModifyColumn(modCol.Column.Name, modColDef);
                    _catalog.Flush();
                    messages.Add($"Modified column '{modCol.Column.Name}'");
                    actionsPerformed++;
                    break;

                case ChangeColumnAction changeCol:
                    if (schema.GetColumnOrdinal(changeCol.OldColumnName) < 0)
                        throw new ColumnNotFoundException(changeCol.OldColumnName, stmt.TableName);
                    
                    var changeColDef = ConvertColumnDefToStorageDefinition(changeCol.NewColumn);
                    schema.ModifyColumn(changeCol.OldColumnName, changeColDef);
                    _catalog.Flush();
                    messages.Add($"Changed column '{changeCol.OldColumnName}' to '{changeCol.NewColumn.Name}'");
                    actionsPerformed++;
                    break;

                case RenameColumnAction renameCol:
                    if (schema.GetColumnOrdinal(renameCol.OldName) < 0)
                        throw new ColumnNotFoundException(renameCol.OldName, stmt.TableName);
                    
                    schema.RenameColumn(renameCol.OldName, renameCol.NewName);
                    _catalog.Flush();
                    messages.Add($"Renamed column '{renameCol.OldName}' to '{renameCol.NewName}'");
                    actionsPerformed++;
                    break;

                case RenameTableAction renameTable:
                    messages.Add($"Renamed table to '{renameTable.NewName}'");
                    actionsPerformed++;
                    break;

                case AddIndexAction addIdx:
                    var idxName = addIdx.IndexName ?? $"idx_{stmt.TableName}_{string.Join("_", addIdx.Columns)}";
                    messages.Add($"Added index '{idxName}'");
                    actionsPerformed++;
                    break;

                case DropIndexAction dropIdx:
                    messages.Add($"Dropped index '{dropIdx.IndexName}'");
                    actionsPerformed++;
                    break;

                case AddPrimaryKeyAction addPk:
                    messages.Add($"Added PRIMARY KEY ({string.Join(", ", addPk.Columns)})");
                    actionsPerformed++;
                    break;

                case DropPrimaryKeyAction:
                    messages.Add("Dropped PRIMARY KEY");
                    actionsPerformed++;
                    break;

                case AddForeignKeyAction addFk:
                    {
                        var fkDef = ForeignKeyDefinition.FromAction(stmt.TableName, addFk);
                        var db = _catalog.GetDatabase(dbName);
                        if (db != null)
                        {
                            db.AddForeignKey(fkDef);
                            _foreignKeyManager.AddForeignKey(
                                fkDef.ConstraintName,
                                dbName,
                                stmt.TableName,
                                addFk.Columns,
                                dbName, // referenced database is same as current
                                addFk.ReferencedTable,
                                addFk.ReferencedColumns,
                                ConvertReferentialAction(addFk.OnDelete),
                                ConvertReferentialAction(addFk.OnUpdate));
                            _catalog.Flush();
                        }
                        messages.Add($"Added FOREIGN KEY '{fkDef.ConstraintName}'");
                        actionsPerformed++;
                    }
                    break;

                case DropForeignKeyAction dropFk:
                    {
                        var db = _catalog.GetDatabase(dbName);
                        if (db != null)
                        {
                            db.RemoveForeignKey(dropFk.ConstraintName);
                            _foreignKeyManager.DropForeignKey(dbName, dropFk.ConstraintName);
                            _catalog.Flush();
                        }
                        messages.Add($"Dropped FOREIGN KEY '{dropFk.ConstraintName}'");
                        actionsPerformed++;
                    }
                    break;

                case AddConstraintAction addConstr:
                    {
                        var db = _catalog.GetDatabase(dbName);
                        if (db != null)
                        {
                            switch (addConstr.Constraint.Type)
                            {
                                case ConstraintType.ForeignKey:
                                    var fkDef = ForeignKeyDefinition.FromConstraint(stmt.TableName, addConstr.Constraint);
                                    db.AddForeignKey(fkDef);
                                    _foreignKeyManager.AddForeignKey(
                                        fkDef.ConstraintName,
                                        dbName,
                                        stmt.TableName,
                                        addConstr.Constraint.Columns,
                                        dbName, // referenced database is same as current
                                        addConstr.Constraint.ReferencedTable!,
                                        addConstr.Constraint.ReferencedColumns,
                                        ConvertReferentialAction(addConstr.Constraint.OnDelete),
                                        ConvertReferentialAction(addConstr.Constraint.OnUpdate));
                                    break;

                                case ConstraintType.Check:
                                    if (addConstr.Constraint.CheckExpression != null)
                                    {
                                        var checkDef = CheckConstraintDefinition.FromConstraint(stmt.TableName, addConstr.Constraint);
                                        db.AddCheckConstraint(checkDef);
                                    }
                                    break;
                            }
                            _catalog.Flush();
                        }
                        messages.Add($"Added constraint '{addConstr.Constraint.Name}'");
                        actionsPerformed++;
                    }
                    break;

                case DropConstraintAction dropConstr:
                    {
                        var db = _catalog.GetDatabase(dbName);
                        if (db != null)
                        {
                            // Try to drop as foreign key first, then as check constraint
                            if (db.HasForeignKey(dropConstr.ConstraintName))
                            {
                                db.RemoveForeignKey(dropConstr.ConstraintName);
                                _foreignKeyManager.DropForeignKey(dbName, dropConstr.ConstraintName);
                            }
                            else if (db.HasCheckConstraint(dropConstr.ConstraintName))
                            {
                                db.RemoveCheckConstraint(dropConstr.ConstraintName);
                            }
                            _catalog.Flush();
                        }
                        messages.Add($"Dropped constraint '{dropConstr.ConstraintName}'");
                        actionsPerformed++;
                    }
                    break;

                default:
                    _logger.Warning("Unhandled ALTER TABLE action: {0}", action.GetType().Name);
                    break;
            }
        }

        _logger.Info("ALTER TABLE {0}.{1}: {2}", dbName, stmt.TableName, string.Join("; ", messages));
        return ExecutionResult.Ddl($"Table '{stmt.TableName}' altered ({actionsPerformed} action(s))");
    }

    /// <summary>
    /// Converts an AST ColumnDef to a storage ColumnDefinition.
    /// </summary>
    private ColumnDefinition ConvertColumnDefToStorageDefinition(Parsing.Ast.ColumnDef colDef)
    {
        DataValue? defaultValue = null;
        if (colDef.DefaultValue != null)
        {
            defaultValue = EvaluateLiteralExpression(colDef.DefaultValue);
        }

        return new ColumnDefinition(
            name: colDef.Name,
            dataType: colDef.DataType,
            maxLength: colDef.Length ?? 255,
            precision: colDef.Precision ?? 10,
            scale: colDef.Scale ?? 0,
            isNullable: colDef.IsNullable,
            isPrimaryKey: colDef.IsPrimaryKey,
            isAutoIncrement: colDef.IsAutoIncrement,
            defaultValue: defaultValue
        );
    }

    /// <summary>
    /// Converts an AST ForeignKeyReferentialAction to a storage ForeignKeyAction.
    /// </summary>
    private static ForeignKeyAction ConvertReferentialAction(ForeignKeyReferentialAction action)
    {
        return action switch
        {
            ForeignKeyReferentialAction.Restrict => ForeignKeyAction.Restrict,
            ForeignKeyReferentialAction.NoAction => ForeignKeyAction.NoAction,
            ForeignKeyReferentialAction.Cascade => ForeignKeyAction.Cascade,
            ForeignKeyReferentialAction.SetNull => ForeignKeyAction.SetNull,
            ForeignKeyReferentialAction.SetDefault => ForeignKeyAction.SetDefault,
            _ => ForeignKeyAction.Restrict
        };
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

    private ExecutionResult ExecuteSetTransaction(SetTransactionStatement stmt)
    {
        // Set isolation level for the current transaction if active
        if (stmt.IsolationLevel.HasValue)
        {
            var level = stmt.IsolationLevel.Value switch
            {
                TransactionIsolationLevel.ReadUncommitted => Transactions.IsolationLevel.ReadUncommitted,
                TransactionIsolationLevel.ReadCommitted => Transactions.IsolationLevel.ReadCommitted,
                TransactionIsolationLevel.RepeatableRead => Transactions.IsolationLevel.RepeatableRead,
                TransactionIsolationLevel.Serializable => Transactions.IsolationLevel.Serializable,
                _ => Transactions.IsolationLevel.ReadCommitted
            };

            // If there's an active transaction, update its isolation level
            if (_currentTransaction != null && _currentTransaction.IsActive)
            {
                _currentTransaction.SetIsolationLevel(level);
            }

            // Also set the session default
            _systemVariables.SetSession("transaction_isolation", stmt.IsolationLevel.Value.ToString().ToUpperInvariant().Replace("READ", "READ-").Replace("REPEATABLE", "REPEATABLE-"));
        }

        // Set access mode
        if (stmt.AccessMode.HasValue)
        {
            if (_currentTransaction != null)
            {
                _currentTransaction.IsReadOnly = stmt.AccessMode.Value == TransactionAccessMode.ReadOnly;
            }
            _systemVariables.SetSession("transaction_read_only", stmt.AccessMode.Value == TransactionAccessMode.ReadOnly ? "ON" : "OFF");
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

    #region User Management

    private ExecutionResult ExecuteCreateUser(CreateUserStatement stmt)
    {
        _logger.Info("CREATE USER {0}@{1}", stmt.UserName, stmt.Host);
        
        // Simplified stub implementation - just log success
        return ExecutionResult.Ddl($"User '{stmt.UserName}'@'{stmt.Host}' created");
    }

    private ExecutionResult ExecuteAlterUser(AlterUserStatement stmt)
    {
        _logger.Info("ALTER USER {0}@{1}", stmt.UserName, stmt.Host);
        
        // Simplified stub implementation
        return ExecutionResult.Ddl($"User '{stmt.UserName}'@'{stmt.Host}' altered");
    }

    private ExecutionResult ExecuteDropUser(DropUserStatement stmt)
    {
        _logger.Info("DROP USER {0}@{1}", stmt.UserName, stmt.Host);
        
        // Simplified stub implementation
        return ExecutionResult.Ddl($"User '{stmt.UserName}'@'{stmt.Host}' dropped");
    }

    private ExecutionResult ExecuteGrant(GrantStatement stmt)
    {
        foreach (var privilegeStr in stmt.Privileges)
        {
            var privilege = UserManager.ParsePrivilege(privilegeStr);
            UserManager.Instance.GrantPrivilege(
                stmt.UserName, 
                stmt.Host, 
                privilege, 
                stmt.DatabaseName, 
                stmt.TableName);
        }

        var privileges = string.Join(", ", stmt.Privileges);
        var target = stmt.TableName != null 
            ? $"{stmt.DatabaseName ?? "*"}.{stmt.TableName}" 
            : $"{stmt.DatabaseName ?? "*"}.*";
        
        _logger.Info("GRANT {0} ON {1} TO {2}@{3}", privileges, target, stmt.UserName, stmt.Host);
        return ExecutionResult.Ddl($"Granted {privileges} on {target} to '{stmt.UserName}'@'{stmt.Host}'");
    }

    private ExecutionResult ExecuteRevoke(RevokeStatement stmt)
    {
        foreach (var privilegeStr in stmt.Privileges)
        {
            var privilege = UserManager.ParsePrivilege(privilegeStr);
            UserManager.Instance.RevokePrivilege(
                stmt.UserName, 
                stmt.Host, 
                privilege, 
                stmt.DatabaseName, 
                stmt.TableName);
        }

        var privileges = string.Join(", ", stmt.Privileges);
        var target = stmt.TableName != null 
            ? $"{stmt.DatabaseName ?? "*"}.{stmt.TableName}" 
            : $"{stmt.DatabaseName ?? "*"}.*";
        
        _logger.Info("REVOKE {0} ON {1} FROM {2}@{3}", privileges, target, stmt.UserName, stmt.Host);
        return ExecutionResult.Ddl($"Revoked {privileges} on {target} from '{stmt.UserName}'@'{stmt.Host}'");
    }

    #endregion

    #region Stored Procedures and Functions

    // Context for procedure execution
    private Dictionary<string, DataValue>? _procedureVariables;
    private DataValue? _procedureReturnValue;
    private string? _leaveLabel;
    private string? _iterateLabel;

    private ExecutionResult ExecuteCreateProcedure(CreateProcedureStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        // Check if procedure already exists
        if (db.HasProcedure(stmt.ProcedureName) && !stmt.OrReplace)
            throw new CyscaleException($"Procedure '{stmt.ProcedureName}' already exists", ErrorCode.ProcedureExists);

        // Create procedure info
        var procInfo = new ProcedureInfo(
            db.GetNextProcedureId(),
            stmt.ProcedureName,
            isFunction: false,
            stmt.Parameters,
            stmt.Body,
            definer: stmt.Definer,
            sqlSecurity: stmt.SqlSecurity,
            comment: stmt.Comment);

        if (stmt.OrReplace)
        {
            db.AddOrReplaceProcedure(procInfo);
        }
        else
        {
            db.AddProcedure(procInfo);
        }

        // Save catalog
        _catalog.SaveCatalog();

        _logger.Info("Created procedure '{0}' in database '{1}'", stmt.ProcedureName, _currentDatabase);
        return ExecutionResult.Ddl($"Procedure '{stmt.ProcedureName}' created successfully");
    }

    private ExecutionResult ExecuteDropProcedure(DropProcedureStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        if (!db.HasProcedure(stmt.ProcedureName))
        {
            if (stmt.IfExists)
            {
                _logger.Info("Procedure '{0}' does not exist (IF EXISTS specified)", stmt.ProcedureName);
                return ExecutionResult.Ddl($"Procedure '{stmt.ProcedureName}' does not exist");
            }
            throw new CyscaleException($"Procedure '{stmt.ProcedureName}' does not exist", ErrorCode.ProcedureNotFound);
        }

        db.RemoveProcedure(stmt.ProcedureName);

        // Save catalog
        _catalog.SaveCatalog();

        _logger.Info("Dropped procedure '{0}' from database '{1}'", stmt.ProcedureName, _currentDatabase);
        return ExecutionResult.Ddl($"Procedure '{stmt.ProcedureName}' dropped successfully");
    }

    private ExecutionResult ExecuteCreateFunction(CreateFunctionStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        // Check if function already exists
        if (db.HasProcedure(stmt.FunctionName) && !stmt.OrReplace)
            throw new CyscaleException($"Function '{stmt.FunctionName}' already exists", ErrorCode.ProcedureExists);

        // Create function info
        var funcInfo = new ProcedureInfo(
            db.GetNextProcedureId(),
            stmt.FunctionName,
            isFunction: true,
            stmt.Parameters,
            stmt.Body,
            returnType: stmt.ReturnType,
            returnSize: stmt.ReturnSize,
            returnScale: stmt.ReturnScale,
            isDeterministic: stmt.IsDeterministic,
            definer: stmt.Definer,
            sqlSecurity: stmt.SqlSecurity,
            comment: stmt.Comment);

        if (stmt.OrReplace)
        {
            db.AddOrReplaceProcedure(funcInfo);
        }
        else
        {
            db.AddProcedure(funcInfo);
        }

        // Save catalog
        _catalog.SaveCatalog();

        _logger.Info("Created function '{0}' in database '{1}'", stmt.FunctionName, _currentDatabase);
        return ExecutionResult.Ddl($"Function '{stmt.FunctionName}' created successfully");
    }

    private ExecutionResult ExecuteDropFunction(DropFunctionStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        if (!db.HasProcedure(stmt.FunctionName))
        {
            if (stmt.IfExists)
            {
                _logger.Info("Function '{0}' does not exist (IF EXISTS specified)", stmt.FunctionName);
                return ExecutionResult.Ddl($"Function '{stmt.FunctionName}' does not exist");
            }
            throw new CyscaleException($"Function '{stmt.FunctionName}' does not exist", ErrorCode.ProcedureNotFound);
        }

        var proc = db.GetProcedure(stmt.FunctionName);
        if (proc != null && !proc.IsFunction)
        {
            throw new CyscaleException($"'{stmt.FunctionName}' is not a function", ErrorCode.InternalError);
        }

        db.RemoveProcedure(stmt.FunctionName);

        // Save catalog
        _catalog.SaveCatalog();

        _logger.Info("Dropped function '{0}' from database '{1}'", stmt.FunctionName, _currentDatabase);
        return ExecutionResult.Ddl($"Function '{stmt.FunctionName}' dropped successfully");
    }

    #region Trigger Execution

    private ExecutionResult ExecuteCreateTrigger(CreateTriggerStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        // Verify the table exists
        if (!db.HasTable(stmt.TableName))
            throw new TableNotFoundException(stmt.TableName);

        // Check if trigger already exists
        if (db.HasTrigger(stmt.TriggerName) && !stmt.OrReplace)
            throw new CyscaleException($"Trigger '{stmt.TriggerName}' already exists", ErrorCode.TriggerExists);

        // Create trigger info
        var triggerInfo = new TriggerInfo(
            db.GetNextTriggerId(),
            stmt.TriggerName,
            stmt.TableName,
            stmt.Timing,
            stmt.Event,
            stmt.Body,
            stmt.Definer);

        if (stmt.OrReplace)
        {
            db.AddOrReplaceTrigger(triggerInfo);
        }
        else
        {
            db.AddTrigger(triggerInfo);
        }

        // Save catalog
        _catalog.SaveCatalog();

        _logger.Info("Created trigger '{0}' on table '{1}' in database '{2}'", 
            stmt.TriggerName, stmt.TableName, _currentDatabase);
        return ExecutionResult.Ddl($"Trigger '{stmt.TriggerName}' created successfully");
    }

    private ExecutionResult ExecuteDropTrigger(DropTriggerStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        if (!db.HasTrigger(stmt.TriggerName))
        {
            if (stmt.IfExists)
                return ExecutionResult.Ddl($"Trigger '{stmt.TriggerName}' does not exist");

            throw new CyscaleException($"Trigger '{stmt.TriggerName}' does not exist", ErrorCode.TriggerNotFound);
        }

        db.RemoveTrigger(stmt.TriggerName);

        // Save catalog
        _catalog.SaveCatalog();

        _logger.Info("Dropped trigger '{0}' from database '{1}'", stmt.TriggerName, _currentDatabase);
        return ExecutionResult.Ddl($"Trigger '{stmt.TriggerName}' dropped successfully");
    }

    /// <summary>
    /// Executes triggers for a specific table, timing, and event.
    /// </summary>
    private void ExecuteTriggers(string tableName, TriggerTiming timing, TriggerEvent @event, 
        Row? oldRow = null, Row? newRow = null)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            return;

        var triggers = db.GetTriggers(tableName, timing, @event);
        
        foreach (var trigger in triggers)
        {
            ExecuteSingleTrigger(trigger, oldRow, newRow);
        }
    }

    /// <summary>
    /// Executes a single trigger.
    /// </summary>
    private void ExecuteSingleTrigger(TriggerInfo trigger, Row? oldRow, Row? newRow)
    {
        // Save current context (support nested triggers)
        var savedVariables = _procedureVariables;
        var savedReturnValue = _procedureReturnValue;
        var savedLeaveLabel = _leaveLabel;
        var savedIterateLabel = _iterateLabel;

        try
        {
            // Initialize trigger execution context
            _procedureVariables = new Dictionary<string, DataValue>(StringComparer.OrdinalIgnoreCase);
            _procedureReturnValue = null;
            _leaveLabel = null;
            _iterateLabel = null;

            // Make OLD and NEW row data available
            // OLD row values are prefixed with "OLD."
            // NEW row values are prefixed with "NEW."
            if (oldRow != null)
            {
                for (int i = 0; i < oldRow.Schema.Columns.Count; i++)
                {
                    var colName = oldRow.Schema.Columns[i].Name;
                    _procedureVariables[$"OLD.{colName}"] = oldRow.Values[i];
                }
            }

            if (newRow != null)
            {
                for (int i = 0; i < newRow.Schema.Columns.Count; i++)
                {
                    var colName = newRow.Schema.Columns[i].Name;
                    _procedureVariables[$"NEW.{colName}"] = newRow.Values[i];
                }
            }

            // Execute trigger body
            foreach (var bodyStmt in trigger.Body)
            {
                Execute(bodyStmt);
            }
        }
        finally
        {
            // Restore previous context
            _procedureVariables = savedVariables;
            _procedureReturnValue = savedReturnValue;
            _leaveLabel = savedLeaveLabel;
            _iterateLabel = savedIterateLabel;
        }
    }

    #endregion

    #region Event Execution

    private ExecutionResult ExecuteCreateEvent(CreateEventStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        // Check if event already exists
        if (db.HasEvent(stmt.EventName) && !stmt.OrReplace)
            throw new CyscaleException($"Event '{stmt.EventName}' already exists", ErrorCode.EventExists);

        // Create event info
        var eventInfo = new EventInfo(
            db.GetNextEventId(),
            stmt.EventName,
            stmt.Schedule,
            stmt.Body,
            stmt.OnCompletionPreserve,
            stmt.Enabled,
            stmt.Comment,
            stmt.Definer);

        if (stmt.OrReplace)
        {
            db.AddOrReplaceEvent(eventInfo);
        }
        else
        {
            db.AddEvent(eventInfo);
        }

        // Save catalog
        _catalog.SaveCatalog();

        _logger.Info("Created event '{0}' in database '{1}'", stmt.EventName, _currentDatabase);
        return ExecutionResult.Ddl($"Event '{stmt.EventName}' created successfully");
    }

    private ExecutionResult ExecuteDropEvent(DropEventStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        if (!db.HasEvent(stmt.EventName))
        {
            if (stmt.IfExists)
                return ExecutionResult.Ddl($"Event '{stmt.EventName}' does not exist");

            throw new CyscaleException($"Event '{stmt.EventName}' does not exist", ErrorCode.EventNotFound);
        }

        db.RemoveEvent(stmt.EventName);

        // Save catalog
        _catalog.SaveCatalog();

        _logger.Info("Dropped event '{0}' from database '{1}'", stmt.EventName, _currentDatabase);
        return ExecutionResult.Ddl($"Event '{stmt.EventName}' dropped successfully");
    }

    /// <summary>
    /// Executes a scheduled event.
    /// </summary>
    internal void ExecuteScheduledEvent(EventInfo eventInfo)
    {
        // Save current context
        var savedVariables = _procedureVariables;
        var savedReturnValue = _procedureReturnValue;
        var savedLeaveLabel = _leaveLabel;
        var savedIterateLabel = _iterateLabel;

        try
        {
            // Initialize event execution context
            _procedureVariables = new Dictionary<string, DataValue>(StringComparer.OrdinalIgnoreCase);
            _procedureReturnValue = null;
            _leaveLabel = null;
            _iterateLabel = null;

            // Execute event body
            foreach (var bodyStmt in eventInfo.Body)
            {
                Execute(bodyStmt);
            }

            // Update event after execution
            eventInfo.UpdateAfterExecution();

            _logger.Info("Executed scheduled event '{0}'", eventInfo.EventName);
        }
        catch (Exception ex)
        {
            _logger.Error("Error executing event '{0}': {1}", eventInfo.EventName, ex.Message);
        }
        finally
        {
            // Restore previous context
            _procedureVariables = savedVariables;
            _procedureReturnValue = savedReturnValue;
            _leaveLabel = savedLeaveLabel;
            _iterateLabel = savedIterateLabel;
        }
    }

    #endregion

    #region Admin Statement Execution

    private ExecutionResult ExecuteAnalyzeTable(AnalyzeTableStatement stmt)
    {
        foreach (var tableName in stmt.TableNames)
        {
            var table = _catalog.GetTable(_currentDatabase, tableName);
            if (table == null)
                throw new TableNotFoundException(tableName);

            // In a real implementation, this would update table statistics
            // For now, we just log the action
            _logger.Info("Analyzed table '{0}' in database '{1}'", tableName, _currentDatabase);
        }

        return ExecutionResult.Ddl($"Analyzed {stmt.TableNames.Count} table(s)");
    }

    private ExecutionResult ExecuteFlush(FlushStatement stmt)
    {
        switch (stmt.FlushType.ToUpperInvariant())
        {
            case "TABLES":
                if (stmt.TableNames.Count > 0)
                {
                    foreach (var tableName in stmt.TableNames)
                    {
                        var table = _catalog.GetTable(_currentDatabase, tableName);
                        table?.Flush();
                    }
                }
                else
                {
                    _catalog.Flush();
                }
                _logger.Info("Flushed tables");
                break;

            case "PRIVILEGES":
                // Reload privilege tables (no-op for now)
                _logger.Info("Flushed privileges");
                break;

            case "LOGS":
                // Close and reopen log files (no-op for now)
                _logger.Info("Flushed logs");
                break;

            case "STATUS":
                // Reset status variables (no-op for now)
                _logger.Info("Flushed status");
                break;
        }

        return ExecutionResult.Ddl($"Flushed {stmt.FlushType}");
    }

    private ExecutionResult ExecuteLockTables(LockTablesStatement stmt)
    {
        // In a real implementation, this would acquire table-level locks
        // For now, we just validate the tables exist and log the action
        foreach (var tableLock in stmt.TableLocks)
        {
            var table = _catalog.GetTable(_currentDatabase, tableLock.TableName);
            if (table == null)
                throw new TableNotFoundException(tableLock.TableName);

            _logger.Info("Locked table '{0}' with {1} lock", tableLock.TableName, tableLock.LockType);
        }

        return ExecutionResult.Ddl($"Locked {stmt.TableLocks.Count} table(s)");
    }

    private ExecutionResult ExecuteUnlockTables()
    {
        // In a real implementation, this would release all table-level locks
        _logger.Info("Unlocked all tables");
        return ExecutionResult.Ddl("Unlocked all tables");
    }

    #endregion

    private ExecutionResult ExecuteCall(CallStatement stmt)
    {
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new DatabaseNotFoundException(_currentDatabase);

        // Get the procedure
        var proc = db.GetProcedure(stmt.ProcedureName);
        if (proc == null)
            throw new CyscaleException($"Procedure '{stmt.ProcedureName}' does not exist", ErrorCode.ProcedureNotFound);

        if (proc.IsFunction)
            throw new CyscaleException($"'{stmt.ProcedureName}' is a function, not a procedure. Use SELECT to call functions.", ErrorCode.InternalError);

        // Check parameter count
        if (stmt.Arguments.Count != proc.Parameters.Count)
            throw new CyscaleException($"Procedure '{stmt.ProcedureName}' expects {proc.Parameters.Count} parameters, got {stmt.Arguments.Count}", ErrorCode.InternalError);

        // Initialize procedure execution context
        _procedureVariables = new Dictionary<string, DataValue>(StringComparer.OrdinalIgnoreCase);
        _procedureReturnValue = null;
        _leaveLabel = null;
        _iterateLabel = null;

        // Evaluate arguments and bind to parameters
        for (int i = 0; i < proc.Parameters.Count; i++)
        {
            var param = proc.Parameters[i];
            var arg = stmt.Arguments[i];

            // For now, we'll just evaluate the argument expression without a row context
            // In a full implementation, this would need proper context handling
            var evaluator = BuildExpressionEvaluator(arg);
            var (_, emptyRow) = CreateDummyContext();
            var value = evaluator.Evaluate(emptyRow);

            _procedureVariables[param.Name] = value;
        }

        // Execute procedure body
        ExecutionResult result = ExecutionResult.Empty();
        foreach (var bodyStmt in proc.Body)
        {
            result = Execute(bodyStmt);

            // Check for early return
            if (_procedureReturnValue != null)
                break;
        }

        // Clean up execution context
        _procedureVariables = null;
        _procedureReturnValue = null;

        _logger.Info("Called procedure '{0}'", stmt.ProcedureName);
        return result;
    }

    private ExecutionResult ExecuteDeclareVariable(DeclareVariableStatement stmt)
    {
        if (_procedureVariables == null)
            throw new CyscaleException("DECLARE can only be used inside a stored procedure or function", ErrorCode.InternalError);

        // Declare variables with default value
        foreach (var varName in stmt.VariableNames)
        {
            DataValue defaultValue;
            if (stmt.DefaultValue != null)
            {
                var evaluator = BuildExpressionEvaluator(stmt.DefaultValue);
                var (_, emptyRow) = CreateDummyContext();
                defaultValue = evaluator.Evaluate(emptyRow);
            }
            else
            {
                defaultValue = DataValue.Null;
            }

            _procedureVariables[varName] = defaultValue;
        }

        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteIf(IfStatement stmt)
    {
        // Evaluate the main IF condition
        var conditionEvaluator = BuildExpressionEvaluator(stmt.Condition);
        var (_, emptyRow) = CreateDummyContext();
        var conditionValue = conditionEvaluator.Evaluate(emptyRow);

        if (conditionValue.AsBoolean())
        {
            // Execute THEN statements
            ExecutionResult result = ExecutionResult.Empty();
            foreach (var thenStmt in stmt.ThenStatements)
            {
                result = Execute(thenStmt);
                if (_procedureReturnValue != null || _leaveLabel != null || _iterateLabel != null)
                    return result;
            }
            return result;
        }

        // Check ELSEIF clauses
        foreach (var (elseIfCondition, elseIfStatements) in stmt.ElseIfClauses)
        {
            var elseIfEvaluator = BuildExpressionEvaluator(elseIfCondition);
            var (_, emptyRow2) = CreateDummyContext();
            var elseIfValue = elseIfEvaluator.Evaluate(emptyRow2);

            if (elseIfValue.AsBoolean())
            {
                ExecutionResult result = ExecutionResult.Empty();
                foreach (var elseIfStmt in elseIfStatements)
                {
                    result = Execute(elseIfStmt);
                    if (_procedureReturnValue != null || _leaveLabel != null || _iterateLabel != null)
                        return result;
                }
                return result;
            }
        }

        // Execute ELSE statements if present
        if (stmt.ElseStatements != null)
        {
            ExecutionResult result = ExecutionResult.Empty();
            foreach (var elseStmt in stmt.ElseStatements)
            {
                result = Execute(elseStmt);
                if (_procedureReturnValue != null || _leaveLabel != null || _iterateLabel != null)
                    return result;
            }
            return result;
        }

        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteWhile(WhileStatement stmt)
    {
        ExecutionResult result = ExecutionResult.Empty();
        var (_, emptyRow) = CreateDummyContext();

        while (true)
        {
            // Evaluate condition
            var conditionEvaluator = BuildExpressionEvaluator(stmt.Condition);
            var conditionValue = conditionEvaluator.Evaluate(emptyRow);

            if (!conditionValue.AsBoolean())
                break;

            // Execute loop body
            foreach (var bodyStmt in stmt.Body)
            {
                result = Execute(bodyStmt);

                // Check for LEAVE (exit loop)
                if (_leaveLabel != null)
                {
                    if (stmt.Label == null || _leaveLabel == stmt.Label)
                    {
                        _leaveLabel = null;
                        return result;
                    }
                    return result; // Propagate LEAVE to outer loop
                }

                // Check for ITERATE (continue to next iteration)
                if (_iterateLabel != null)
                {
                    if (stmt.Label == null || _iterateLabel == stmt.Label)
                    {
                        _iterateLabel = null;
                        break; // Continue to next iteration
                    }
                    return result; // Propagate ITERATE to outer loop
                }

                // Check for RETURN
                if (_procedureReturnValue != null)
                    return result;
            }
        }

        return result;
    }

    private ExecutionResult ExecuteRepeat(RepeatStatement stmt)
    {
        ExecutionResult result = ExecutionResult.Empty();

        while (true)
        {
            // Execute loop body
            foreach (var bodyStmt in stmt.Body)
            {
                result = Execute(bodyStmt);

                // Check for LEAVE (exit loop)
                if (_leaveLabel != null)
                {
                    if (stmt.Label == null || _leaveLabel == stmt.Label)
                    {
                        _leaveLabel = null;
                        return result;
                    }
                    return result; // Propagate LEAVE to outer loop
                }

                // Check for ITERATE (continue to next iteration)
                if (_iterateLabel != null)
                {
                    if (stmt.Label == null || _iterateLabel == stmt.Label)
                    {
                        _iterateLabel = null;
                        break; // Continue to next iteration
                    }
                    return result; // Propagate ITERATE to outer loop
                }

                // Check for RETURN
                if (_procedureReturnValue != null)
                    return result;
            }

            // Evaluate UNTIL condition (loop continues until this is true)
            var untilEvaluator = BuildExpressionEvaluator(stmt.UntilCondition);
            var (_, emptyRow) = CreateDummyContext();
            var untilValue = untilEvaluator.Evaluate(emptyRow);

            if (untilValue.AsBoolean())
                break;
        }

        return result;
    }

    private ExecutionResult ExecuteLoop(LoopStatement stmt)
    {
        ExecutionResult result = ExecutionResult.Empty();

        while (true)
        {
            // Execute loop body
            foreach (var bodyStmt in stmt.Body)
            {
                result = Execute(bodyStmt);

                // Check for LEAVE (exit loop)
                if (_leaveLabel != null)
                {
                    if (_leaveLabel == stmt.Label)
                    {
                        _leaveLabel = null;
                        return result;
                    }
                    return result; // Propagate LEAVE to outer loop
                }

                // Check for ITERATE (continue to next iteration)
                if (_iterateLabel != null)
                {
                    if (_iterateLabel == stmt.Label)
                    {
                        _iterateLabel = null;
                        break; // Continue to next iteration
                    }
                    return result; // Propagate ITERATE to outer loop
                }

                // Check for RETURN
                if (_procedureReturnValue != null)
                    return result;
            }
        }
    }

    private ExecutionResult ExecuteLeave(LeaveStatement stmt)
    {
        _leaveLabel = stmt.Label;
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteIterate(IterateStatement stmt)
    {
        _iterateLabel = stmt.Label;
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteReturn(ReturnStatement stmt)
    {
        if (_procedureVariables == null)
            throw new CyscaleException("RETURN can only be used inside a stored function", ErrorCode.InternalError);

        var evaluator = BuildExpressionEvaluator(stmt.Value);
        var (_, emptyRow) = CreateDummyContext();
        _procedureReturnValue = evaluator.Evaluate(emptyRow);

        return ExecutionResult.Empty();
    }

    /// <summary>
    /// Executes a user-defined function and returns its result.
    /// Called by UserFunctionEvaluator during expression evaluation.
    /// </summary>
    internal DataValue ExecuteUserFunction(ProcedureInfo function, List<DataValue> argValues)
    {
        // Save current procedure context (support nested function calls)
        var savedVariables = _procedureVariables;
        var savedReturnValue = _procedureReturnValue;
        var savedLeaveLabel = _leaveLabel;
        var savedIterateLabel = _iterateLabel;

        try
        {
            // Initialize function execution context
            _procedureVariables = new Dictionary<string, DataValue>(StringComparer.OrdinalIgnoreCase);
            _procedureReturnValue = null;
            _leaveLabel = null;
            _iterateLabel = null;

            // Bind arguments to parameters
            for (int i = 0; i < function.Parameters.Count; i++)
            {
                var param = function.Parameters[i];
                _procedureVariables[param.Name] = argValues[i];
            }

            // Execute function body
            foreach (var bodyStmt in function.Body)
            {
                Execute(bodyStmt);

                // Check for RETURN
                if (_procedureReturnValue != null)
                    break;
            }

            // Return the result (or NULL if no RETURN was executed)
            return _procedureReturnValue ?? DataValue.Null;
        }
        finally
        {
            // Restore previous procedure context
            _procedureVariables = savedVariables;
            _procedureReturnValue = savedReturnValue;
            _leaveLabel = savedLeaveLabel;
            _iterateLabel = savedIterateLabel;
        }
    }

    #endregion

    #region Expression Building

    /// <summary>
    /// Creates a dummy schema and row for evaluating expressions in stored procedures.
    /// </summary>
    private (TableSchema Schema, Row Row) CreateDummyContext()
    {
        var dummyColumn = new ColumnDefinition("_dummy", DataType.Int);
        var emptySchema = new TableSchema(0, "_proc", "_context", [dummyColumn]);
        var emptyRow = new Row(emptySchema, [DataValue.Null]);
        return (emptySchema, emptyRow);
    }

    /// <summary>
    /// Builds an expression evaluator without a specific table schema (for stored procedures).
    /// </summary>
    private IExpressionEvaluator BuildExpressionEvaluator(Expression expr)
    {
        // Create a minimal dummy schema for expressions that don't reference columns
        var (schema, _) = CreateDummyContext();
        return BuildExpression(expr, schema);
    }

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
            CaseExpression caseExpr => BuildCaseExpression(caseExpr, schema),
            LikeExpression likeExpr => BuildLikeExpression(likeExpr, schema),
            MatchExpression matchExpr => BuildMatchExpression(matchExpr, schema),
            ExistsExpression existsExpr => BuildExistsExpression(existsExpr, schema),
            QuantifiedComparisonExpression quantified => BuildQuantifiedComparisonExpression(quantified, schema),
            Subquery subquery => BuildSubqueryExpression(subquery, schema),
            WindowFunctionCall windowFunc => BuildWindowFunctionPlaceholder(windowFunc, schema),
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
            "ISNULL" => BuildIsNullFunction(func, schema),
            "IF" => BuildIfFunction(func, schema),
            "FIELD" => BuildFieldFunction(func, schema),
            // JSON functions
            "JSON_EXTRACT" => BuildJsonExtractFunction(func, schema),
            "JSON_UNQUOTE" => BuildJsonUnquoteFunction(func, schema),
            "JSON_OBJECT" => BuildJsonObjectFunction(func, schema),
            "JSON_ARRAY" => BuildJsonArrayFunction(func, schema),
            "JSON_SET" => BuildJsonSetFunction(func, schema),
            "JSON_INSERT" => BuildJsonInsertFunction(func, schema),
            "JSON_REPLACE" => BuildJsonReplaceFunction(func, schema),
            "JSON_REMOVE" => BuildJsonRemoveFunction(func, schema),
            "JSON_KEYS" => BuildJsonKeysFunction(func, schema),
            "JSON_LENGTH" => BuildJsonLengthFunction(func, schema),
            "JSON_VALID" => BuildJsonValidFunction(func, schema),
            "JSON_TYPE" => BuildJsonTypeFunction(func, schema),
            "JSON_CONTAINS" => BuildJsonContainsFunction(func, schema),
            "JSON_CONTAINS_PATH" => BuildJsonContainsPathFunction(func, schema),
            "JSON_SEARCH" => BuildJsonSearchFunction(func, schema),
            "JSON_MERGE_PATCH" => BuildJsonMergePatchFunction(func, schema),
            "JSON_MERGE_PRESERVE" or "JSON_MERGE" => BuildJsonMergePreserveFunction(func, schema),
            // Spatial functions
            "ST_POINT" or "POINT" => BuildStPointFunction(func, schema),
            "ST_GEOMFROMTEXT" or "ST_GEOMETRYFROMTEXT" => BuildStGeomFromTextFunction(func, schema),
            "ST_ASTEXT" or "ST_ASWKT" => BuildStAsTextFunction(func, schema),
            "ST_DISTANCE" => BuildStDistanceFunction(func, schema),
            "ST_DISTANCE_SPHERE" => BuildStDistanceSphereFunction(func, schema),
            "ST_CONTAINS" => BuildStContainsFunction(func, schema),
            "ST_WITHIN" => BuildStWithinFunction(func, schema),
            "ST_INTERSECTS" => BuildStIntersectsFunction(func, schema),
            "ST_BUFFER" => BuildStBufferFunction(func, schema),
            "ST_X" => BuildStXFunction(func, schema),
            "ST_Y" => BuildStYFunction(func, schema),
            "ST_SRID" => BuildStSridFunction(func, schema),
            _ when IsAggregateFunction(funcName) => 
                // Aggregates in non-GROUP BY context - return constant for now
                new ConstantEvaluator(DataValue.Null),
            _ => BuildUserDefinedFunction(func, schema)
        };
    }

    /// <summary>
    /// Builds an evaluator for a user-defined function (stored function).
    /// </summary>
    private IExpressionEvaluator BuildUserDefinedFunction(FunctionCall func, TableSchema schema)
    {
        // Look up the function in the current database
        var db = _catalog.GetDatabase(_currentDatabase);
        if (db == null)
            throw new CyscaleException($"Unknown function: {func.FunctionName}");

        var funcInfo = db.GetProcedure(func.FunctionName);
        if (funcInfo == null || !funcInfo.IsFunction)
            throw new CyscaleException($"Unknown function: {func.FunctionName}");

        // Check parameter count
        if (func.Arguments.Count != funcInfo.Parameters.Count)
            throw new CyscaleException($"Function '{func.FunctionName}' expects {funcInfo.Parameters.Count} arguments, got {func.Arguments.Count}");

        // Build evaluators for all arguments
        var argEvaluators = new List<IExpressionEvaluator>();
        foreach (var arg in func.Arguments)
        {
            argEvaluators.Add(BuildExpression(arg, schema));
        }

        return new UserFunctionEvaluator(funcInfo, argEvaluators, this);
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

    private IExpressionEvaluator BuildIsNullFunction(FunctionCall func, TableSchema schema)
    {
        // ISNULL(expr) returns 1 if expr is NULL, 0 otherwise
        if (func.Arguments.Count < 1)
            throw new CyscaleException("ISNULL requires 1 argument");

        var expr = BuildExpression(func.Arguments[0], schema);
        return new IsNullFunctionEvaluator(expr);
    }

    private IExpressionEvaluator BuildFieldFunction(FunctionCall func, TableSchema schema)
    {
        // FIELD(str, str1, str2, ...) returns index position of str in str1,str2,... list (1-based, 0 if not found)
        if (func.Arguments.Count < 2)
            throw new CyscaleException("FIELD requires at least 2 arguments");

        var search = BuildExpression(func.Arguments[0], schema);
        var list = func.Arguments.Skip(1).Select(a => BuildExpression(a, schema)).ToList();
        return new FieldFunctionEvaluator(search, list);
    }

    #region JSON Function Builders

    private IExpressionEvaluator BuildJsonExtractFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("JSON_EXTRACT requires 2 arguments (json_doc, path)");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var path = BuildExpression(func.Arguments[1], schema);
        return new JsonExtractEvaluator(jsonDoc, path);
    }

    private IExpressionEvaluator BuildJsonUnquoteFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("JSON_UNQUOTE requires 1 argument");

        var arg = BuildExpression(func.Arguments[0], schema);
        return new JsonUnquoteEvaluator(arg);
    }

    private IExpressionEvaluator BuildJsonObjectFunction(FunctionCall func, TableSchema schema)
    {
        // JSON_OBJECT(key1, val1, key2, val2, ...)
        if (func.Arguments.Count % 2 != 0)
            throw new CyscaleException("JSON_OBJECT requires even number of arguments (key-value pairs)");

        var args = func.Arguments.Select(a => BuildExpression(a, schema)).ToList();
        return new JsonObjectEvaluator(args);
    }

    private IExpressionEvaluator BuildJsonArrayFunction(FunctionCall func, TableSchema schema)
    {
        // JSON_ARRAY(val1, val2, ...)
        var args = func.Arguments.Select(a => BuildExpression(a, schema)).ToList();
        return new JsonArrayEvaluator(args);
    }

    private IExpressionEvaluator BuildJsonSetFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 3)
            throw new CyscaleException("JSON_SET requires at least 3 arguments (json_doc, path, value)");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var args = func.Arguments.Skip(1).Select(a => BuildExpression(a, schema)).ToList();
        return new JsonSetEvaluator(jsonDoc, args, JsonModifyMode.Set);
    }

    private IExpressionEvaluator BuildJsonInsertFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 3)
            throw new CyscaleException("JSON_INSERT requires at least 3 arguments");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var args = func.Arguments.Skip(1).Select(a => BuildExpression(a, schema)).ToList();
        return new JsonSetEvaluator(jsonDoc, args, JsonModifyMode.Insert);
    }

    private IExpressionEvaluator BuildJsonReplaceFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 3)
            throw new CyscaleException("JSON_REPLACE requires at least 3 arguments");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var args = func.Arguments.Skip(1).Select(a => BuildExpression(a, schema)).ToList();
        return new JsonSetEvaluator(jsonDoc, args, JsonModifyMode.Replace);
    }

    private IExpressionEvaluator BuildJsonRemoveFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("JSON_REMOVE requires at least 2 arguments");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var paths = func.Arguments.Skip(1).Select(a => BuildExpression(a, schema)).ToList();
        return new JsonRemoveEvaluator(jsonDoc, paths);
    }

    private IExpressionEvaluator BuildJsonKeysFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("JSON_KEYS requires at least 1 argument");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var path = func.Arguments.Count > 1 ? BuildExpression(func.Arguments[1], schema) : null;
        return new JsonKeysEvaluator(jsonDoc, path);
    }

    private IExpressionEvaluator BuildJsonLengthFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("JSON_LENGTH requires at least 1 argument");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var path = func.Arguments.Count > 1 ? BuildExpression(func.Arguments[1], schema) : null;
        return new JsonLengthEvaluator(jsonDoc, path);
    }

    private IExpressionEvaluator BuildJsonValidFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("JSON_VALID requires 1 argument");

        var arg = BuildExpression(func.Arguments[0], schema);
        return new JsonValidEvaluator(arg);
    }

    private IExpressionEvaluator BuildJsonTypeFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("JSON_TYPE requires 1 argument");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var path = func.Arguments.Count > 1 ? BuildExpression(func.Arguments[1], schema) : null;
        return new JsonTypeEvaluator(jsonDoc, path);
    }

    private IExpressionEvaluator BuildJsonContainsFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("JSON_CONTAINS requires at least 2 arguments");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var target = BuildExpression(func.Arguments[1], schema);
        var path = func.Arguments.Count > 2 ? BuildExpression(func.Arguments[2], schema) : null;
        return new JsonContainsEvaluator(jsonDoc, target, path);
    }

    private IExpressionEvaluator BuildJsonContainsPathFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 3)
            throw new CyscaleException("JSON_CONTAINS_PATH requires at least 3 arguments");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var oneOrAll = BuildExpression(func.Arguments[1], schema);
        var paths = func.Arguments.Skip(2).Select(a => BuildExpression(a, schema)).ToList();
        return new JsonContainsPathEvaluator(jsonDoc, oneOrAll, paths);
    }

    private IExpressionEvaluator BuildJsonSearchFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 3)
            throw new CyscaleException("JSON_SEARCH requires at least 3 arguments (json_doc, one_or_all, search_str)");

        var jsonDoc = BuildExpression(func.Arguments[0], schema);
        var oneOrAll = BuildExpression(func.Arguments[1], schema);
        var searchStr = BuildExpression(func.Arguments[2], schema);
        var path = func.Arguments.Count > 3 ? BuildExpression(func.Arguments[3], schema) : null;
        return new JsonSearchEvaluator(jsonDoc, oneOrAll, searchStr, path);
    }

    private IExpressionEvaluator BuildJsonMergePatchFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("JSON_MERGE_PATCH requires at least 2 arguments");

        var args = func.Arguments.Select(a => BuildExpression(a, schema)).ToList();
        return new JsonMergePatchEvaluator(args);
    }

    private IExpressionEvaluator BuildJsonMergePreserveFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("JSON_MERGE_PRESERVE requires at least 2 arguments");

        var args = func.Arguments.Select(a => BuildExpression(a, schema)).ToList();
        return new JsonMergePreserveEvaluator(args);
    }

    #endregion

    #region Spatial Functions

    private IExpressionEvaluator BuildStPointFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("ST_POINT requires 2 arguments (x, y)");

        var x = BuildExpression(func.Arguments[0], schema);
        var y = BuildExpression(func.Arguments[1], schema);
        return new StPointEvaluator(x, y);
    }

    private IExpressionEvaluator BuildStGeomFromTextFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("ST_GEOMFROMTEXT requires at least 1 argument");

        var wkt = BuildExpression(func.Arguments[0], schema);
        var srid = func.Arguments.Count > 1 ? BuildExpression(func.Arguments[1], schema) : null;
        return new StGeomFromTextEvaluator(wkt, srid);
    }

    private IExpressionEvaluator BuildStAsTextFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("ST_ASTEXT requires 1 argument");

        var geom = BuildExpression(func.Arguments[0], schema);
        return new StAsTextEvaluator(geom);
    }

    private IExpressionEvaluator BuildStDistanceFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("ST_DISTANCE requires 2 arguments");

        var geom1 = BuildExpression(func.Arguments[0], schema);
        var geom2 = BuildExpression(func.Arguments[1], schema);
        return new StDistanceEvaluator(geom1, geom2);
    }

    private IExpressionEvaluator BuildStDistanceSphereFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("ST_DISTANCE_SPHERE requires 2 arguments");

        var geom1 = BuildExpression(func.Arguments[0], schema);
        var geom2 = BuildExpression(func.Arguments[1], schema);
        return new StDistanceSphereEvaluator(geom1, geom2);
    }

    private IExpressionEvaluator BuildStContainsFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("ST_CONTAINS requires 2 arguments");

        var geom1 = BuildExpression(func.Arguments[0], schema);
        var geom2 = BuildExpression(func.Arguments[1], schema);
        return new StContainsEvaluator(geom1, geom2);
    }

    private IExpressionEvaluator BuildStWithinFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("ST_WITHIN requires 2 arguments");

        var geom1 = BuildExpression(func.Arguments[0], schema);
        var geom2 = BuildExpression(func.Arguments[1], schema);
        return new StWithinEvaluator(geom1, geom2);
    }

    private IExpressionEvaluator BuildStIntersectsFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("ST_INTERSECTS requires 2 arguments");

        var geom1 = BuildExpression(func.Arguments[0], schema);
        var geom2 = BuildExpression(func.Arguments[1], schema);
        return new StIntersectsEvaluator(geom1, geom2);
    }

    private IExpressionEvaluator BuildStBufferFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 2)
            throw new CyscaleException("ST_BUFFER requires 2 arguments");

        var geom = BuildExpression(func.Arguments[0], schema);
        var distance = BuildExpression(func.Arguments[1], schema);
        return new StBufferEvaluator(geom, distance);
    }

    private IExpressionEvaluator BuildStXFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("ST_X requires 1 argument");

        var point = BuildExpression(func.Arguments[0], schema);
        return new StXEvaluator(point);
    }

    private IExpressionEvaluator BuildStYFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("ST_Y requires 1 argument");

        var point = BuildExpression(func.Arguments[0], schema);
        return new StYEvaluator(point);
    }

    private IExpressionEvaluator BuildStSridFunction(FunctionCall func, TableSchema schema)
    {
        if (func.Arguments.Count < 1)
            throw new CyscaleException("ST_SRID requires 1 argument");

        var geom = BuildExpression(func.Arguments[0], schema);
        return new StSridEvaluator(geom);
    }

    #endregion

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

    /// <summary>
    /// Builds a CASE expression evaluator.
    /// Supports both searched CASE (CASE WHEN cond THEN result...) 
    /// and simple CASE (CASE expr WHEN value THEN result...).
    /// </summary>
    private IExpressionEvaluator BuildCaseExpression(CaseExpression caseExpr, TableSchema schema)
    {
        IExpressionEvaluator? operand = null;
        if (caseExpr.Operand != null)
        {
            operand = BuildExpression(caseExpr.Operand, schema);
        }

        var whenClauses = new List<(IExpressionEvaluator When, IExpressionEvaluator Then)>();
        foreach (var whenClause in caseExpr.WhenClauses)
        {
            var when = BuildExpression(whenClause.When, schema);
            var then = BuildExpression(whenClause.Then, schema);
            whenClauses.Add((when, then));
        }

        IExpressionEvaluator? elseResult = null;
        if (caseExpr.ElseResult != null)
        {
            elseResult = BuildExpression(caseExpr.ElseResult, schema);
        }

        return new CaseEvaluator(operand, whenClauses, elseResult);
    }

    /// <summary>
    /// Builds a LIKE expression evaluator.
    /// </summary>
    private IExpressionEvaluator BuildLikeExpression(LikeExpression likeExpr, TableSchema schema)
    {
        var expr = BuildExpression(likeExpr.Expression, schema);
        var pattern = BuildExpression(likeExpr.Pattern, schema);
        return new LikeEvaluator(expr, pattern, likeExpr.IsNot);
    }

    /// <summary>
    /// Builds a MATCH...AGAINST full-text search expression evaluator.
    /// </summary>
    private IExpressionEvaluator BuildMatchExpression(MatchExpression matchExpr, TableSchema schema)
    {
        // Build evaluators for each column
        var columnEvaluators = new List<IExpressionEvaluator>();
        foreach (var col in matchExpr.Columns)
        {
            var colEval = BuildColumnReference(col, schema);
            columnEvaluators.Add(colEval);
        }

        // Build the search text evaluator
        var searchTextEvaluator = BuildExpression(matchExpr.SearchText, schema);

        // Convert the search mode
        var mode = matchExpr.Mode switch
        {
            MatchSearchMode.Boolean => Storage.Index.BooleanSearchMode.And,
            _ => Storage.Index.BooleanSearchMode.Or
        };

        return new MatchAgainstEvaluator(columnEvaluators, searchTextEvaluator, mode);
    }

    /// <summary>
    /// Builds an EXISTS expression evaluator.
    /// </summary>
    private IExpressionEvaluator BuildExistsExpression(ExistsExpression existsExpr, TableSchema schema)
    {
        // Execute the subquery and check if any rows are returned
        var subqueryOp = BuildSelectOperator(existsExpr.Subquery);
        return new ExistsEvaluator(subqueryOp);
    }

    /// <summary>
    /// Builds a quantified comparison expression evaluator (ALL/ANY/SOME).
    /// </summary>
    private IExpressionEvaluator BuildQuantifiedComparisonExpression(QuantifiedComparisonExpression quantified, TableSchema schema)
    {
        var expr = BuildExpression(quantified.Expression, schema);
        var subqueryOp = BuildSelectOperator(quantified.Subquery);
        var op = ConvertBinaryOperator(quantified.Operator);

        return new QuantifiedComparisonEvaluator(expr, subqueryOp, op, quantified.Quantifier);
    }

    /// <summary>
    /// Builds a scalar subquery expression evaluator.
    /// </summary>
    private IExpressionEvaluator BuildSubqueryExpression(Subquery subquery, TableSchema schema)
    {
        // Detect if this is a correlated subquery
        // A correlated subquery references columns from the outer query's schema
        var isCorrelated = IsCorrelatedSubquery(subquery.Query, schema);

        // Build and execute the subquery
        var subqueryOp = BuildSelectOperator(subquery.Query);
        return new SubqueryEvaluator(subqueryOp, isCorrelated);
    }

    /// <summary>
    /// Checks if a subquery is correlated (references columns from the outer query).
    /// </summary>
    private bool IsCorrelatedSubquery(SelectStatement subquery, TableSchema outerSchema)
    {
        // Check WHERE clause for references to outer columns
        if (subquery.Where != null && ContainsOuterReference(subquery.Where, outerSchema))
            return true;

        // Check SELECT columns
        foreach (var col in subquery.Columns)
        {
            if (col.Expression != null && ContainsOuterReference(col.Expression, outerSchema))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Checks if an expression contains references to columns from the outer schema.
    /// </summary>
    private bool ContainsOuterReference(Expression expr, TableSchema outerSchema)
    {
        return expr switch
        {
            ColumnReference col => outerSchema.GetColumnOrdinal(col.ColumnName) >= 0,
            BinaryExpression bin => ContainsOuterReference(bin.Left, outerSchema) || 
                                    ContainsOuterReference(bin.Right, outerSchema),
            UnaryExpression un => ContainsOuterReference(un.Operand, outerSchema),
            FunctionCall func => func.Arguments.Any(a => ContainsOuterReference(a, outerSchema)),
            InExpression inExpr => ContainsOuterReference(inExpr.Expression, outerSchema) ||
                                   (inExpr.Values?.Any(v => ContainsOuterReference(v, outerSchema)) ?? false),
            BetweenExpression between => ContainsOuterReference(between.Expression, outerSchema) ||
                                         ContainsOuterReference(between.Low, outerSchema) ||
                                         ContainsOuterReference(between.High, outerSchema),
            CaseExpression caseExpr => (caseExpr.Operand != null && ContainsOuterReference(caseExpr.Operand, outerSchema)) ||
                                       caseExpr.WhenClauses.Any(w => ContainsOuterReference(w.When, outerSchema) || 
                                                                     ContainsOuterReference(w.Then, outerSchema)) ||
                                       (caseExpr.ElseResult != null && ContainsOuterReference(caseExpr.ElseResult, outerSchema)),
            Subquery => true, // Nested subqueries are considered correlated for safety
            _ => false
        };
    }

    /// <summary>
    /// Builds a placeholder for window functions.
    /// Window functions are computed at the operator level, so this returns a constant
    /// that will be replaced by the actual value during WindowOperator execution.
    /// </summary>
    private IExpressionEvaluator BuildWindowFunctionPlaceholder(WindowFunctionCall windowFunc, TableSchema schema)
    {
        // Window functions need special handling - they require the full partition to compute.
        // For now, we return a placeholder. The actual computation happens in WindowOperator.
        // The result column will be added by the WindowOperator.
        var columnName = $"{windowFunc.FunctionName}()";
        
        // Try to find if this column already exists in the schema (from a previous WindowOperator)
        var ordinal = schema.GetColumnOrdinal(columnName);
        if (ordinal >= 0)
        {
            return new ColumnEvaluator(ordinal);
        }
        
        // Return a placeholder that computes the function
        // This is used when the window function needs to be evaluated inline
        _logger.Debug("Window function {0} will be computed by WindowOperator", windowFunc.FunctionName);
        return new ConstantEvaluator(DataValue.Null);
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

/// <summary>
/// Evaluates ISNULL function - returns 1 if expr is NULL, 0 otherwise.
/// Note: This is different from IFNULL(expr, default) which replaces NULL values.
/// </summary>
internal sealed class IsNullFunctionEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expression;

    public IsNullFunctionEvaluator(IExpressionEvaluator expression)
    {
        _expression = expression;
    }

    public DataValue Evaluate(Row row)
    {
        var val = _expression.Evaluate(row);
        return DataValue.FromInt(val.IsNull ? 1 : 0);
    }
}

/// <summary>
/// Evaluates FIELD function - returns index position (1-based) of search value in list, or 0 if not found.
/// </summary>
internal sealed class FieldFunctionEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _search;
    private readonly List<IExpressionEvaluator> _list;

    public FieldFunctionEvaluator(IExpressionEvaluator search, List<IExpressionEvaluator> list)
    {
        _search = search;
        _list = list;
    }

    public DataValue Evaluate(Row row)
    {
        var searchVal = _search.Evaluate(row);
        if (searchVal.IsNull)
            return DataValue.FromInt(0);

        var searchStr = searchVal.AsString();
        for (int i = 0; i < _list.Count; i++)
        {
            var listVal = _list[i].Evaluate(row);
            if (!listVal.IsNull && string.Equals(searchStr, listVal.AsString(), StringComparison.OrdinalIgnoreCase))
            {
                return DataValue.FromInt(i + 1); // 1-based index
            }
        }
        return DataValue.FromInt(0);
    }
}

/// <summary>
/// Evaluates CASE expressions.
/// Supports both searched CASE (CASE WHEN cond THEN result...) 
/// and simple CASE (CASE expr WHEN value THEN result...).
/// </summary>
internal sealed class CaseEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator? _operand;
    private readonly List<(IExpressionEvaluator When, IExpressionEvaluator Then)> _whenClauses;
    private readonly IExpressionEvaluator? _elseResult;

    public CaseEvaluator(
        IExpressionEvaluator? operand,
        List<(IExpressionEvaluator When, IExpressionEvaluator Then)> whenClauses,
        IExpressionEvaluator? elseResult)
    {
        _operand = operand;
        _whenClauses = whenClauses;
        _elseResult = elseResult;
    }

    public DataValue Evaluate(Row row)
    {
        if (_operand != null)
        {
            // Simple CASE: CASE operand WHEN value1 THEN result1 ...
            var operandValue = _operand.Evaluate(row);
            
            foreach (var (when, then) in _whenClauses)
            {
                var whenValue = when.Evaluate(row);
                if (operandValue.Equals(whenValue))
                {
                    return then.Evaluate(row);
                }
            }
        }
        else
        {
            // Searched CASE: CASE WHEN condition1 THEN result1 ...
            foreach (var (when, then) in _whenClauses)
            {
                var condition = when.Evaluate(row);
                if (!condition.IsNull && condition.Type == DataType.Boolean && condition.AsBoolean())
                {
                    return then.Evaluate(row);
                }
                // Also handle truthy non-boolean values (e.g., 1 for true)
                if (!condition.IsNull && condition.Type != DataType.Boolean)
                {
                    var isTruthy = condition.Type switch
                    {
                        DataType.Int => condition.AsInt() != 0,
                        DataType.BigInt => condition.AsBigInt() != 0,
                        DataType.TinyInt => condition.AsTinyInt() != 0,
                        DataType.SmallInt => condition.AsSmallInt() != 0,
                        _ => true
                    };
                    if (isTruthy)
                    {
                        return then.Evaluate(row);
                    }
                }
            }
        }

        // No match found, return ELSE result or NULL
        return _elseResult?.Evaluate(row) ?? DataValue.Null;
    }
}

/// <summary>
/// Evaluates LIKE expressions with pattern matching.
/// Supports % (any characters) and _ (single character) wildcards.
/// </summary>
internal sealed class LikeEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expression;
    private readonly IExpressionEvaluator _pattern;
    private readonly bool _isNot;

    public LikeEvaluator(IExpressionEvaluator expression, IExpressionEvaluator pattern, bool isNot)
    {
        _expression = expression;
        _pattern = pattern;
        _isNot = isNot;
    }

    public DataValue Evaluate(Row row)
    {
        var value = _expression.Evaluate(row);
        var pattern = _pattern.Evaluate(row);

        if (value.IsNull || pattern.IsNull)
            return DataValue.Null;

        var valueStr = value.AsString();
        var patternStr = pattern.AsString();
        var matches = MatchLikePattern(valueStr, patternStr);

        return DataValue.FromBoolean(_isNot ? !matches : matches);
    }

    /// <summary>
    /// Matches a string against a SQL LIKE pattern.
    /// % matches any sequence of characters, _ matches any single character.
    /// </summary>
    private static bool MatchLikePattern(string value, string pattern)
    {
        // Convert SQL LIKE pattern to regex
        var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";

        return System.Text.RegularExpressions.Regex.IsMatch(
            value, 
            regexPattern, 
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }
}

/// <summary>
/// Evaluates MATCH...AGAINST full-text search expressions.
/// Returns a relevance score based on the search text and column content.
/// </summary>
internal sealed class MatchAgainstEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _columnEvaluators;
    private readonly IExpressionEvaluator _searchTextEvaluator;
    private readonly Storage.Index.BooleanSearchMode _mode;
    private readonly Storage.Index.SimpleTokenizer _tokenizer;

    public MatchAgainstEvaluator(
        List<IExpressionEvaluator> columnEvaluators,
        IExpressionEvaluator searchTextEvaluator,
        Storage.Index.BooleanSearchMode mode)
    {
        _columnEvaluators = columnEvaluators;
        _searchTextEvaluator = searchTextEvaluator;
        _mode = mode;
        _tokenizer = new Storage.Index.SimpleTokenizer();
    }

    public DataValue Evaluate(Row row)
    {
        var searchTextValue = _searchTextEvaluator.Evaluate(row);
        if (searchTextValue.IsNull)
            return DataValue.FromDouble(0.0);

        var searchText = searchTextValue.AsString();
        if (string.IsNullOrWhiteSpace(searchText))
            return DataValue.FromDouble(0.0);

        // Tokenize the search text
        var searchTokens = _tokenizer.Tokenize(searchText)
            .Select(t => t.Term)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (searchTokens.Count == 0)
            return DataValue.FromDouble(0.0);

        // Collect content from all columns
        var contentBuilder = new System.Text.StringBuilder();
        foreach (var colEval in _columnEvaluators)
        {
            var colValue = colEval.Evaluate(row);
            if (!colValue.IsNull)
            {
                if (contentBuilder.Length > 0)
                    contentBuilder.Append(' ');
                contentBuilder.Append(colValue.AsString());
            }
        }

        var content = contentBuilder.ToString();
        if (string.IsNullOrWhiteSpace(content))
            return DataValue.FromDouble(0.0);

        // Tokenize the content
        var contentTokens = _tokenizer.Tokenize(content)
            .Select(t => t.Term)
            .ToList();

        if (contentTokens.Count == 0)
            return DataValue.FromDouble(0.0);

        // Calculate relevance score using TF-IDF-like approach
        var termFrequencies = contentTokens
            .GroupBy(t => t, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);

        double score = 0;
        int matchedTerms = 0;

        foreach (var searchTerm in searchTokens)
        {
            if (termFrequencies.TryGetValue(searchTerm, out var tf))
            {
                // Normalized TF = frequency / document length
                double normalizedTf = (double)tf / contentTokens.Count;
                score += normalizedTf;
                matchedTerms++;
            }
        }

        // In boolean AND mode, all terms must match
        if (_mode == Storage.Index.BooleanSearchMode.And && matchedTerms < searchTokens.Count)
        {
            return DataValue.FromDouble(0.0);
        }

        // Normalize by number of search terms
        return DataValue.FromDouble(score / searchTokens.Count);
    }
}

/// <summary>
/// Evaluates EXISTS expressions by checking if a subquery returns any rows.
/// </summary>
internal sealed class ExistsEvaluator : IExpressionEvaluator
{
    private readonly IOperator _subquery;

    public ExistsEvaluator(IOperator subquery)
    {
        _subquery = subquery;
    }

    public DataValue Evaluate(Row row)
    {
        try
        {
            _subquery.Open();
            var result = _subquery.Next();
            var exists = result != null;
            _subquery.Close();
            return DataValue.FromBoolean(exists);
        }
        finally
        {
            _subquery.Dispose();
        }
    }
}

/// <summary>
/// Evaluates scalar subquery expressions by returning the first value from the subquery.
/// For non-correlated subqueries, the result is cached after first execution.
/// </summary>
internal sealed class SubqueryEvaluator : IExpressionEvaluator
{
    private readonly IOperator _subquery;
    private bool _isCorrelated;
    private bool _hasCached;
    private DataValue _cachedResult;
    private readonly Dictionary<string, DataValue> _correlatedCache;

    public SubqueryEvaluator(IOperator subquery, bool isCorrelated = false)
    {
        _subquery = subquery;
        _isCorrelated = isCorrelated;
        _hasCached = false;
        _cachedResult = DataValue.Null;
        _correlatedCache = new Dictionary<string, DataValue>();
    }

    public DataValue Evaluate(Row row)
    {
        // For non-correlated subqueries, return cached result if available
        if (!_isCorrelated && _hasCached)
        {
            return _cachedResult;
        }

        // For correlated subqueries, check if we've seen this row key before
        if (_isCorrelated && row != null)
        {
            var rowKey = ComputeRowKey(row);
            if (_correlatedCache.TryGetValue(rowKey, out var cachedValue))
            {
                return cachedValue;
            }
        }

        try
        {
            _subquery.Open();
            var result = _subquery.Next();
            _subquery.Close();

            DataValue value;
            if (result == null)
            {
                value = DataValue.Null;
            }
            else
            {
                // Return the first column value
                value = result.Values.Length > 0 ? result.Values[0] : DataValue.Null;
            }

            // Cache the result
            if (!_isCorrelated)
            {
                _cachedResult = value;
                _hasCached = true;
            }
            else if (row != null)
            {
                var rowKey = ComputeRowKey(row);
                _correlatedCache[rowKey] = value;
            }

            return value;
        }
        finally
        {
            _subquery.Dispose();
        }
    }

    /// <summary>
    /// Computes a cache key based on row values for correlated subquery caching.
    /// </summary>
    private static string ComputeRowKey(Row row)
    {
        // Use a simple hash of all column values
        var sb = new System.Text.StringBuilder();
        foreach (var val in row.Values)
        {
            sb.Append(val.ToString());
            sb.Append('|');
        }
        return sb.ToString();
    }
}

/// <summary>
/// Evaluates quantified comparison expressions (ALL/ANY/SOME).
/// Examples:
/// - x > ALL (SELECT y FROM t): True if x > every value in subquery
/// - x = ANY (SELECT y FROM t): True if x equals at least one value in subquery
/// - x < SOME (SELECT y FROM t): Same as ANY (SOME is synonym for ANY)
/// </summary>
internal sealed class QuantifiedComparisonEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _expression;
    private readonly IOperator _subquery;
    private readonly BinaryOp _operator;
    private readonly QuantifierType _quantifier;

    public QuantifiedComparisonEvaluator(
        IExpressionEvaluator expression, 
        IOperator subquery, 
        BinaryOp @operator, 
        QuantifierType quantifier)
    {
        _expression = expression;
        _subquery = subquery;
        _operator = @operator;
        _quantifier = quantifier;
    }

    public DataValue Evaluate(Row row)
    {
        var leftValue = _expression.Evaluate(row);

        // Collect all values from subquery
        var subqueryValues = new List<DataValue>();
        
        try
        {
            _subquery.Open();
            Row? subqueryRow;
            while ((subqueryRow = _subquery.Next()) != null)
            {
                if (subqueryRow.Values.Length > 0)
                {
                    subqueryValues.Add(subqueryRow.Values[0]);
                }
            }
            _subquery.Close();
        }
        finally
        {
            _subquery.Dispose();
        }

        // Empty subquery handling
        if (subqueryValues.Count == 0)
        {
            // For ALL: empty set means condition is vacuously true
            // For ANY/SOME: empty set means no match, so false
            return _quantifier == QuantifierType.All 
                ? DataValue.FromBoolean(true) 
                : DataValue.FromBoolean(false);
        }

        // Evaluate based on quantifier
        switch (_quantifier)
        {
            case QuantifierType.All:
                // Must be true for ALL values
                foreach (var rightValue in subqueryValues)
                {
                    if (!EvaluateComparison(leftValue, rightValue, _operator))
                    {
                        return DataValue.FromBoolean(false);
                    }
                }
                return DataValue.FromBoolean(true);

            case QuantifierType.Any:
            case QuantifierType.Some:
                // Must be true for at least ONE value
                foreach (var rightValue in subqueryValues)
                {
                    if (EvaluateComparison(leftValue, rightValue, _operator))
                    {
                        return DataValue.FromBoolean(true);
                    }
                }
                return DataValue.FromBoolean(false);

            default:
                return DataValue.Null;
        }
    }

    /// <summary>
    /// Evaluates a comparison between two values.
    /// </summary>
    private static bool EvaluateComparison(DataValue left, DataValue right, BinaryOp op)
    {
        // Handle NULL: comparisons with NULL return unknown (treated as false)
        if (left.IsNull || right.IsNull)
            return false;

        return op switch
        {
            BinaryOp.Equal => left == right,
            BinaryOp.NotEqual => left != right,
            BinaryOp.LessThan => left < right,
            BinaryOp.LessThanOrEqual => left <= right,
            BinaryOp.GreaterThan => left > right,
            BinaryOp.GreaterThanOrEqual => left >= right,
            _ => false
        };
    }
}

/// <summary>
/// Mode for JSON modification functions (SET, INSERT, REPLACE).
/// </summary>
internal enum JsonModifyMode
{
    /// <summary>
    /// JSON_SET: creates or replaces value at path.
    /// </summary>
    Set,

    /// <summary>
    /// JSON_INSERT: only creates value if path doesn't exist.
    /// </summary>
    Insert,

    /// <summary>
    /// JSON_REPLACE: only replaces value if path exists.
    /// </summary>
    Replace
}

/// <summary>
/// Evaluates JSON_EXTRACT function - extracts value from JSON document using path.
/// </summary>
internal sealed class JsonExtractEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly IExpressionEvaluator _path;

    public JsonExtractEvaluator(IExpressionEvaluator jsonDoc, IExpressionEvaluator path)
    {
        _jsonDoc = jsonDoc;
        _path = path;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        var pathValue = _path.Evaluate(row);

        if (jsonValue.IsNull || pathValue.IsNull)
            return DataValue.Null;

        try
        {
            var jsonText = jsonValue.AsString();
            var pathText = pathValue.AsString();

            using var doc = System.Text.Json.JsonDocument.Parse(jsonText);
            var result = ExtractJsonPath(doc.RootElement, pathText);

            return result.HasValue 
                ? DataValue.FromVarChar(result.Value.ToString()) 
                : DataValue.Null;
        }
        catch
        {
            return DataValue.Null;
        }
    }

    private static System.Text.Json.JsonElement? ExtractJsonPath(System.Text.Json.JsonElement root, string path)
    {
        if (!path.StartsWith("$.") && path != "$")
            return null;

        if (path == "$")
            return root;

        var keys = path[2..].Split('.');
        var current = root;

        foreach (var key in keys)
        {
            if (current.ValueKind == System.Text.Json.JsonValueKind.Object)
            {
                if (current.TryGetProperty(key, out var property))
                {
                    current = property;
                }
                else
                {
                    return null;
                }
            }
            else if (current.ValueKind == System.Text.Json.JsonValueKind.Array && int.TryParse(key, out var index))
            {
                if (index >= 0 && index < current.GetArrayLength())
                {
                    current = current[index];
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        return current;
    }
}

/// <summary>
/// Evaluates JSON_UNQUOTE function - removes quotes and unescapes JSON string.
/// </summary>
internal sealed class JsonUnquoteEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;

    public JsonUnquoteEvaluator(IExpressionEvaluator arg)
    {
        _arg = arg;
    }

    public DataValue Evaluate(Row row)
    {
        var value = _arg.Evaluate(row);
        if (value.IsNull)
            return DataValue.Null;

        var str = value.AsString();
        
        if (str.Length >= 2 && str[0] == '"' && str[^1] == '"')
        {
            str = str[1..^1];
        }

        str = System.Text.RegularExpressions.Regex.Unescape(str);
        return DataValue.FromVarChar(str);
    }
}

/// <summary>
/// Evaluates JSON_OBJECT function - creates JSON object from key-value pairs.
/// </summary>
internal sealed class JsonObjectEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;

    public JsonObjectEvaluator(List<IExpressionEvaluator> args)
    {
        _args = args;
    }

    public DataValue Evaluate(Row row)
    {
        var obj = new Dictionary<string, object?>();

        for (int i = 0; i < _args.Count; i += 2)
        {
            var key = _args[i].Evaluate(row).AsString();
            var value = _args[i + 1].Evaluate(row);
            obj[key] = value.IsNull ? null : value.AsString();
        }

        var json = System.Text.Json.JsonSerializer.Serialize(obj);
        return DataValue.FromVarChar(json);
    }
}

/// <summary>
/// Evaluates JSON_ARRAY function - creates JSON array from values.
/// </summary>
internal sealed class JsonArrayEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;

    public JsonArrayEvaluator(List<IExpressionEvaluator> args)
    {
        _args = args;
    }

    public DataValue Evaluate(Row row)
    {
        var array = new List<object?>();

        foreach (var arg in _args)
        {
            var value = arg.Evaluate(row);
            array.Add(value.IsNull ? null : value.AsString());
        }

        var json = System.Text.Json.JsonSerializer.Serialize(array);
        return DataValue.FromVarChar(json);
    }
}

/// <summary>
/// Evaluates JSON_SET, JSON_INSERT, JSON_REPLACE functions.
/// </summary>
internal sealed class JsonSetEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly List<IExpressionEvaluator> _args;
    private readonly JsonModifyMode _mode;

    public JsonSetEvaluator(IExpressionEvaluator jsonDoc, List<IExpressionEvaluator> args, JsonModifyMode mode)
    {
        _jsonDoc = jsonDoc;
        _args = args;
        _mode = mode;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        if (jsonValue.IsNull)
            return DataValue.Null;

        // Simplified implementation - return original for now
        return jsonValue;
    }
}

/// <summary>
/// Evaluates JSON_REMOVE function.
/// </summary>
internal sealed class JsonRemoveEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly List<IExpressionEvaluator> _paths;

    public JsonRemoveEvaluator(IExpressionEvaluator jsonDoc, List<IExpressionEvaluator> paths)
    {
        _jsonDoc = jsonDoc;
        _paths = paths;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        if (jsonValue.IsNull)
            return DataValue.Null;

        return jsonValue;
    }
}

/// <summary>
/// Evaluates JSON_KEYS function - returns array of keys in JSON object.
/// </summary>
internal sealed class JsonKeysEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly IExpressionEvaluator? _path;

    public JsonKeysEvaluator(IExpressionEvaluator jsonDoc, IExpressionEvaluator? path)
    {
        _jsonDoc = jsonDoc;
        _path = path;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        if (jsonValue.IsNull)
            return DataValue.Null;

        try
        {
            var jsonText = jsonValue.AsString();
            using var doc = System.Text.Json.JsonDocument.Parse(jsonText);
            var root = doc.RootElement;

            if (root.ValueKind == System.Text.Json.JsonValueKind.Object)
            {
                var keys = new List<string>();
                foreach (var property in root.EnumerateObject())
                {
                    keys.Add(property.Name);
                }
                var keysJson = System.Text.Json.JsonSerializer.Serialize(keys);
                return DataValue.FromVarChar(keysJson);
            }

            return DataValue.Null;
        }
        catch
        {
            return DataValue.Null;
        }
    }
}

/// <summary>
/// Evaluates JSON_LENGTH function - returns length of JSON array or object.
/// </summary>
internal sealed class JsonLengthEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly IExpressionEvaluator? _path;

    public JsonLengthEvaluator(IExpressionEvaluator jsonDoc, IExpressionEvaluator? path)
    {
        _jsonDoc = jsonDoc;
        _path = path;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        if (jsonValue.IsNull)
            return DataValue.Null;

        try
        {
            var jsonText = jsonValue.AsString();
            using var doc = System.Text.Json.JsonDocument.Parse(jsonText);
            var root = doc.RootElement;

            if (root.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                return DataValue.FromInt(root.GetArrayLength());
            }
            else if (root.ValueKind == System.Text.Json.JsonValueKind.Object)
            {
                int count = 0;
                foreach (var _ in root.EnumerateObject())
                {
                    count++;
                }
                return DataValue.FromInt(count);
            }

            return DataValue.FromInt(1);
        }
        catch
        {
            return DataValue.Null;
        }
    }
}

/// <summary>
/// Evaluates JSON_VALID function - checks if string is valid JSON.
/// </summary>
internal sealed class JsonValidEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _arg;

    public JsonValidEvaluator(IExpressionEvaluator arg)
    {
        _arg = arg;
    }

    public DataValue Evaluate(Row row)
    {
        var value = _arg.Evaluate(row);
        if (value.IsNull)
            return DataValue.FromInt(0);

        try
        {
            System.Text.Json.JsonDocument.Parse(value.AsString());
            return DataValue.FromInt(1);
        }
        catch
        {
            return DataValue.FromInt(0);
        }
    }
}

/// <summary>
/// Evaluates JSON_TYPE function - returns type of JSON value.
/// </summary>
internal sealed class JsonTypeEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly IExpressionEvaluator? _path;

    public JsonTypeEvaluator(IExpressionEvaluator jsonDoc, IExpressionEvaluator? path)
    {
        _jsonDoc = jsonDoc;
        _path = path;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        if (jsonValue.IsNull)
            return DataValue.Null;

        try
        {
            var jsonText = jsonValue.AsString();
            using var doc = System.Text.Json.JsonDocument.Parse(jsonText);
            var root = doc.RootElement;

            var typeName = root.ValueKind switch
            {
                System.Text.Json.JsonValueKind.Object => "OBJECT",
                System.Text.Json.JsonValueKind.Array => "ARRAY",
                System.Text.Json.JsonValueKind.String => "STRING",
                System.Text.Json.JsonValueKind.Number => "NUMBER",
                System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False => "BOOLEAN",
                System.Text.Json.JsonValueKind.Null => "NULL",
                _ => "UNKNOWN"
            };

            return DataValue.FromVarChar(typeName);
        }
        catch
        {
            return DataValue.Null;
        }
    }
}

/// <summary>
/// Evaluates JSON_CONTAINS function - checks if JSON document contains target.
/// </summary>
internal sealed class JsonContainsEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly IExpressionEvaluator _target;
    private readonly IExpressionEvaluator? _path;

    public JsonContainsEvaluator(IExpressionEvaluator jsonDoc, IExpressionEvaluator target, IExpressionEvaluator? path)
    {
        _jsonDoc = jsonDoc;
        _target = target;
        _path = path;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        var targetValue = _target.Evaluate(row);

        if (jsonValue.IsNull || targetValue.IsNull)
            return DataValue.FromInt(0);

        try
        {
            var jsonText = jsonValue.AsString();
            var targetText = targetValue.AsString();

            return DataValue.FromInt(jsonText.Contains(targetText) ? 1 : 0);
        }
        catch
        {
            return DataValue.FromInt(0);
        }
    }
}

/// <summary>
/// Evaluates JSON_CONTAINS_PATH function - checks if paths exist in JSON.
/// </summary>
internal sealed class JsonContainsPathEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly IExpressionEvaluator _oneOrAll;
    private readonly List<IExpressionEvaluator> _paths;

    public JsonContainsPathEvaluator(IExpressionEvaluator jsonDoc, IExpressionEvaluator oneOrAll, List<IExpressionEvaluator> paths)
    {
        _jsonDoc = jsonDoc;
        _oneOrAll = oneOrAll;
        _paths = paths;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        if (jsonValue.IsNull)
            return DataValue.FromInt(0);

        return DataValue.FromInt(0);
    }
}

/// <summary>
/// Evaluates JSON_SEARCH function - searches for a string in JSON and returns its path.
/// </summary>
internal sealed class JsonSearchEvaluator : IExpressionEvaluator
{
    private readonly IExpressionEvaluator _jsonDoc;
    private readonly IExpressionEvaluator _oneOrAll;
    private readonly IExpressionEvaluator _searchStr;
    private readonly IExpressionEvaluator? _path;

    public JsonSearchEvaluator(IExpressionEvaluator jsonDoc, IExpressionEvaluator oneOrAll, 
        IExpressionEvaluator searchStr, IExpressionEvaluator? path)
    {
        _jsonDoc = jsonDoc;
        _oneOrAll = oneOrAll;
        _searchStr = searchStr;
        _path = path;
    }

    public DataValue Evaluate(Row row)
    {
        var jsonValue = _jsonDoc.Evaluate(row);
        var oneOrAllValue = _oneOrAll.Evaluate(row);
        var searchValue = _searchStr.Evaluate(row);

        if (jsonValue.IsNull || searchValue.IsNull)
            return DataValue.Null;

        try
        {
            var jsonText = jsonValue.AsString();
            var searchText = searchValue.AsString();
            var oneOrAll = oneOrAllValue.AsString().ToUpperInvariant();
            var startPath = _path?.Evaluate(row).AsString() ?? "$";

            using var doc = System.Text.Json.JsonDocument.Parse(jsonText);
            var results = SearchJson(doc.RootElement, searchText, startPath, oneOrAll == "ALL");

            if (results.Count == 0)
                return DataValue.Null;

            if (oneOrAll == "ONE")
                return DataValue.FromVarChar($"\"{results[0]}\"");

            // Return array of all matching paths
            var pathsJson = "[" + string.Join(", ", results.Select(p => $"\"{p}\"")) + "]";
            return DataValue.FromVarChar(pathsJson);
        }
        catch
        {
            return DataValue.Null;
        }
    }

    private static List<string> SearchJson(System.Text.Json.JsonElement element, string searchStr, 
        string currentPath, bool findAll)
    {
        var results = new List<string>();
        SearchJsonRecursive(element, searchStr, currentPath, findAll, results);
        return results;
    }

    private static void SearchJsonRecursive(System.Text.Json.JsonElement element, string searchStr,
        string currentPath, bool findAll, List<string> results)
    {
        switch (element.ValueKind)
        {
            case System.Text.Json.JsonValueKind.String:
                var strValue = element.GetString();
                if (strValue != null && MatchesPattern(strValue, searchStr))
                {
                    results.Add(currentPath);
                    if (!findAll) return;
                }
                break;

            case System.Text.Json.JsonValueKind.Array:
                int index = 0;
                foreach (var item in element.EnumerateArray())
                {
                    SearchJsonRecursive(item, searchStr, $"{currentPath}[{index}]", findAll, results);
                    if (!findAll && results.Count > 0) return;
                    index++;
                }
                break;

            case System.Text.Json.JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                {
                    SearchJsonRecursive(prop.Value, searchStr, $"{currentPath}.{prop.Name}", findAll, results);
                    if (!findAll && results.Count > 0) return;
                }
                break;
        }
    }

    private static bool MatchesPattern(string value, string pattern)
    {
        // Support SQL LIKE-style wildcards: % matches any sequence, _ matches single char
        if (!pattern.Contains('%') && !pattern.Contains('_'))
            return value.Equals(pattern, StringComparison.OrdinalIgnoreCase);

        var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";
        return System.Text.RegularExpressions.Regex.IsMatch(value, regexPattern, 
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }
}

/// <summary>
/// Evaluates JSON_MERGE_PATCH function - RFC 7396 JSON Merge Patch.
/// Recursively merges JSON documents, with null values removing keys.
/// </summary>
internal sealed class JsonMergePatchEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;

    public JsonMergePatchEvaluator(List<IExpressionEvaluator> args)
    {
        _args = args;
    }

    public DataValue Evaluate(Row row)
    {
        if (_args.Count == 0)
            return DataValue.Null;

        try
        {
            // Start with the first document
            var result = _args[0].Evaluate(row);
            if (result.IsNull)
                return DataValue.Null;

            var mergedDoc = System.Text.Json.JsonDocument.Parse(result.AsString());
            var merged = JsonElementToNode(mergedDoc.RootElement);

            // Merge each subsequent document
            for (int i = 1; i < _args.Count; i++)
            {
                var patchValue = _args[i].Evaluate(row);
                if (patchValue.IsNull)
                    continue;

                var patchDoc = System.Text.Json.JsonDocument.Parse(patchValue.AsString());
                merged = MergePatch(merged, JsonElementToNode(patchDoc.RootElement));
            }

            return DataValue.FromVarChar(NodeToJson(merged));
        }
        catch
        {
            return DataValue.Null;
        }
    }

    private static object? JsonElementToNode(System.Text.Json.JsonElement element)
    {
        return element.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Null => null,
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Number => element.GetDouble(),
            System.Text.Json.JsonValueKind.String => element.GetString(),
            System.Text.Json.JsonValueKind.Array => element.EnumerateArray()
                .Select(JsonElementToNode).ToList(),
            System.Text.Json.JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(p => p.Name, p => JsonElementToNode(p.Value)),
            _ => null
        };
    }

    private static object? MergePatch(object? target, object? patch)
    {
        // If patch is not an object, it replaces target
        if (patch is not Dictionary<string, object?> patchObj)
            return patch;

        // If target is not an object, use empty object
        var targetObj = target as Dictionary<string, object?> ?? new Dictionary<string, object?>();
        var result = new Dictionary<string, object?>(targetObj);

        foreach (var kvp in patchObj)
        {
            if (kvp.Value == null)
            {
                // null removes the key
                result.Remove(kvp.Key);
            }
            else
            {
                // Recursively merge
                result[kvp.Key] = MergePatch(
                    targetObj.GetValueOrDefault(kvp.Key),
                    kvp.Value);
            }
        }

        return result;
    }

    private static string NodeToJson(object? node)
    {
        return System.Text.Json.JsonSerializer.Serialize(node);
    }
}

/// <summary>
/// Evaluates JSON_MERGE_PRESERVE function - preserves all values including arrays.
/// Arrays are concatenated, objects are merged with array preservation.
/// </summary>
internal sealed class JsonMergePreserveEvaluator : IExpressionEvaluator
{
    private readonly List<IExpressionEvaluator> _args;

    public JsonMergePreserveEvaluator(List<IExpressionEvaluator> args)
    {
        _args = args;
    }

    public DataValue Evaluate(Row row)
    {
        if (_args.Count == 0)
            return DataValue.Null;

        try
        {
            // Start with the first document
            var result = _args[0].Evaluate(row);
            if (result.IsNull)
                return DataValue.Null;

            var mergedDoc = System.Text.Json.JsonDocument.Parse(result.AsString());
            var merged = JsonElementToNode(mergedDoc.RootElement);

            // Merge each subsequent document
            for (int i = 1; i < _args.Count; i++)
            {
                var patchValue = _args[i].Evaluate(row);
                if (patchValue.IsNull)
                    continue;

                var patchDoc = System.Text.Json.JsonDocument.Parse(patchValue.AsString());
                merged = MergePreserve(merged, JsonElementToNode(patchDoc.RootElement));
            }

            return DataValue.FromVarChar(NodeToJson(merged));
        }
        catch
        {
            return DataValue.Null;
        }
    }

    private static object? JsonElementToNode(System.Text.Json.JsonElement element)
    {
        return element.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Null => null,
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Number => element.GetDouble(),
            System.Text.Json.JsonValueKind.String => element.GetString(),
            System.Text.Json.JsonValueKind.Array => element.EnumerateArray()
                .Select(JsonElementToNode).ToList(),
            System.Text.Json.JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(p => p.Name, p => JsonElementToNode(p.Value)),
            _ => null
        };
    }

    private static object? MergePreserve(object? target, object? patch)
    {
        // If both are arrays, concatenate them
        if (target is List<object?> targetList && patch is List<object?> patchList)
        {
            var merged = new List<object?>(targetList);
            merged.AddRange(patchList);
            return merged;
        }

        // If patch is not an object, wrap both in array if target exists
        if (patch is not Dictionary<string, object?> patchObj)
        {
            if (target != null)
            {
                // Convert to array with both values
                return new List<object?> { target, patch };
            }
            return patch;
        }

        // If target is not an object, wrap in array
        if (target is not Dictionary<string, object?> targetObj)
        {
            if (target != null)
            {
                return new List<object?> { target, patch };
            }
            return patch;
        }

        // Both are objects - merge them
        var result = new Dictionary<string, object?>(targetObj);

        foreach (var kvp in patchObj)
        {
            if (targetObj.TryGetValue(kvp.Key, out var existingValue))
            {
                // Key exists - recursively merge
                result[kvp.Key] = MergePreserve(existingValue, kvp.Value);
            }
            else
            {
                // Key doesn't exist - just add it
                result[kvp.Key] = kvp.Value;
            }
        }

        return result;
    }

    private static string NodeToJson(object? node)
    {
        return System.Text.Json.JsonSerializer.Serialize(node);
    }
}

/// <summary>
/// Evaluates user-defined stored functions.
/// </summary>
internal sealed class UserFunctionEvaluator : IExpressionEvaluator
{
    private readonly ProcedureInfo _function;
    private readonly List<IExpressionEvaluator> _argEvaluators;
    private readonly Executor _executor;

    public UserFunctionEvaluator(ProcedureInfo function, List<IExpressionEvaluator> argEvaluators, Executor executor)
    {
        _function = function;
        _argEvaluators = argEvaluators;
        _executor = executor;
    }

    public DataValue Evaluate(Row row)
    {
        // Evaluate arguments using the current row context
        var argValues = new List<DataValue>();
        foreach (var argEval in _argEvaluators)
        {
            argValues.Add(argEval.Evaluate(row));
        }

        // Execute the function and return the result
        return _executor.ExecuteUserFunction(_function, argValues);
    }
}

#endregion
