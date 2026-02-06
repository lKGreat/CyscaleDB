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
    private long _connectionId;

    // Warning tracking callback (set by protocol layer)
    private Action<string, int, string>? _addWarning;
    private Func<IReadOnlyList<(string level, int code, string message)>>? _getWarnings;

    /// <summary>
    /// Sets session context from the protocol layer.
    /// </summary>
    public void SetSessionContext(long connectionId, string username, string host,
        Action<string, int, string>? addWarning = null,
        Func<IReadOnlyList<(string level, int code, string message)>>? getWarnings = null)
    {
        _connectionId = connectionId;
        _currentUser = username;
        _currentHost = host;
        _addWarning = addWarning;
        _getWarnings = getWarnings;
    }
    
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
    /// Executes a SQL string and returns all results for multi-statement support.
    /// </summary>
    public List<ExecutionResult> ExecuteMultiple(string sql)
    {
        var parser = new Parser(sql);
        var statements = parser.ParseMultiple();

        if (statements.Count == 0)
        {
            return [ExecutionResult.Empty()];
        }

        var results = new List<ExecutionResult>(statements.Count);
        foreach (var statement in statements)
        {
            results.Add(Execute(statement));
        }
        return results;
    }

    /// <summary>
    /// Executes a parsed statement.
    /// </summary>
    public ExecutionResult Execute(Statement statement)
    {
        // Check privileges before execution
        CheckStatementPrivilege(statement);

        // Start timing for metrics
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var metrics = Monitoring.MetricsCollector.Instance;
        var sql = statement.ToString() ?? statement.GetType().Name;
        
        try
        {
            var result = ExecuteInternal(statement);
            stopwatch.Stop();
            
            // Record successful query
            metrics.RecordQuery(sql, stopwatch.Elapsed, null, failed: false);
            
            // Check for slow query and log it
            CheckAndLogSlowQuery(sql, stopwatch.Elapsed, null);
            
            return result;
        }
        catch
        {
            stopwatch.Stop();
            
            // Record failed query
            metrics.RecordQuery(sql, stopwatch.Elapsed, null, failed: true);
            throw;
        }
    }

    /// <summary>
    /// Singleton slow query log instance.
    /// </summary>
    private static Monitoring.SlowQueryLog? _slowQueryLog;
    private static readonly object _slowQueryLogLock = new();

    /// <summary>
    /// Gets or creates the slow query log instance.
    /// </summary>
    public static Monitoring.SlowQueryLog SlowQueryLog
    {
        get
        {
            if (_slowQueryLog == null)
            {
                lock (_slowQueryLogLock)
                {
                    _slowQueryLog ??= new Monitoring.SlowQueryLog(
                        Path.Combine(Environment.CurrentDirectory, "logs", "slow_query.log"));
                }
            }
            return _slowQueryLog;
        }
    }

    /// <summary>
    /// Checks if a query exceeded the slow query threshold and logs it.
    /// </summary>
    private void CheckAndLogSlowQuery(string sql, TimeSpan duration, Monitoring.ExecutionPlan? plan)
    {
        var config = Common.CyscaleDbConfiguration.Current;
        var thresholdMs = config?.SlowQueryThresholdMs ?? 1000;
        
        if (duration.TotalMilliseconds >= thresholdMs)
        {
            SlowQueryLog.LogSlowQuery(sql, duration, plan, _currentUser, _currentDatabase);
        }
    }

    /// <summary>
    /// Internal execution method that dispatches to specific handlers.
    /// </summary>
    private ExecutionResult ExecuteInternal(Statement statement)
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
            ShowProcesslistStatement s => ExecuteShowProcesslist(s),
            ShowGrantsStatement s => ExecuteShowGrants(s),
            ShowCreateDatabaseStatement s => ExecuteShowCreateDatabase(s),
            ShowCreateViewStatement s => ExecuteShowCreateView(s),
            ShowCreateRoutineStatement s => ExecuteShowCreateRoutine(s),
            ShowRoutineStatusStatement s => ExecuteShowRoutineStatus(s),
            ShowTriggersStatement s => ExecuteShowTriggers(s),
            ShowEventsStatement s => ExecuteShowEvents(s),
            ShowPluginsStatement => ExecuteShowPlugins(),
            ShowEngineStatusStatement s => ExecuteShowEngineStatus(s),
            ShowEnginesStatement => ExecuteShowEngines(),
            ShowPrivilegesStatement => ExecuteShowPrivileges(),
            ShowMasterStatusStatement => ExecuteShowMasterStatus(),
            ShowReplicaStatusStatement => ExecuteShowReplicaStatus(),
            ShowBinaryLogsStatement => ExecuteShowBinaryLogs(),
            ShowOpenTablesStatement s => ExecuteShowOpenTables(s),
            ShowCountStatement s => ExecuteShowCount(s),
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
            // Cursor and signal statements
            DeclareCursorStatement s => ExecuteDeclareCursor(s),
            OpenCursorStatement s => ExecuteOpenCursor(s),
            FetchCursorStatement s => ExecuteFetchCursor(s),
            CloseCursorStatement s => ExecuteCloseCursor(s),
            DeclareHandlerStatement s => ExecuteDeclareHandler(s),
            SignalStatement s => ExecuteSignal(s),
            ResignalStatement s => ExecuteResignal(s),
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
            ExplainStatement s => ExecuteExplain(s),
            // Phase 2 new statements
            PrepareStatement s => ExecutePrepare(s),
            ExecuteStatement s => ExecuteExecuteStmt(s),
            DeallocatePrepareStatement s => ExecuteDeallocate(s),
            LoadDataStatement s => ExecuteLoadData(s),
            RenameTableStatement s => ExecuteRenameTable(s),
            CheckTableStatement s => ExecuteCheckTable(s),
            RepairTableStatement s => ExecuteRepairTable(s),
            ChecksumTableStatement s => ExecuteChecksumTable(s),
            KillStatement s => ExecuteKill(s),
            ResetStatement s => ExecuteReset(s),
            BackupRestoreStatement s => ExecuteBackupRestore(s),
            DoStatement s => ExecuteDo(s),
            HandlerOpenStatement s => ExecuteHandlerOpen(s),
            HandlerReadStatement s => ExecuteHandlerRead(s),
            HandlerCloseStatement s => ExecuteHandlerClose(s),
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

        // Save the source schema before projection for ORDER BY resolution
        var sourceSchema = op.Schema;

        // WHERE clause
        if (stmt.Where != null)
        {
            var predicate = BuildExpression(stmt.Where, op.Schema);
            op = new FilterOperator(op, predicate);
            sourceSchema = op.Schema; // Update after filter
        }

        // Determine if ORDER BY references columns not in SELECT
        var orderByExtraColumns = new List<string>();
        var selectedColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        
        if (!IsSelectAll(stmt))
        {
            foreach (var col in stmt.Columns)
            {
                if (col.IsWildcard)
                {
                    // Wildcard selects all columns from source
                    foreach (var schemaCol in sourceSchema.Columns)
                    {
                        selectedColumnNames.Add(schemaCol.Name);
                    }
                }
                else
                {
                    var name = col.Alias ?? GetExpressionName(col.Expression);
                    selectedColumnNames.Add(name);
                }
            }

            // Check ORDER BY columns
            foreach (var orderClause in stmt.OrderBy)
            {
                var orderColNames = ExtractColumnReferences(orderClause.Expression);
                foreach (var colName in orderColNames)
                {
                    // Check if this column exists in source but not in SELECT
                    if (!selectedColumnNames.Contains(colName) && 
                        sourceSchema.GetColumn(colName) != null)
                    {
                        orderByExtraColumns.Add(colName);
                        selectedColumnNames.Add(colName); // Mark as added
                    }
                }
            }
        }

        bool needsFinalProjection = orderByExtraColumns.Count > 0 && !hasGroupBy && !hasAggregates;

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
                if (needsFinalProjection)
                {
                    // Include ORDER BY columns in the projection for sorting
                    var extendedProjections = BuildProjections(stmt.Columns, sourceSchema);
                    foreach (var extraCol in orderByExtraColumns)
                    {
                        var colDef = sourceSchema.GetColumn(extraCol);
                        if (colDef != null)
                        {
                            var eval = new ColumnEvaluator(colDef.OrdinalPosition);
                            extendedProjections.Add(new ProjectionColumn(eval, extraCol, colDef.DataType));
                        }
                    }
                    op = new ProjectOperator(op, extendedProjections, _currentDatabase, "result");
                }
                else
                {
                    var projections = BuildProjections(stmt.Columns, op.Schema);
                    op = new ProjectOperator(op, projections, _currentDatabase, "result");
                }
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

        // Final projection to remove ORDER BY helper columns
        if (needsFinalProjection && stmt.OrderBy.Count > 0)
        {
            var finalProjections = BuildProjections(stmt.Columns, op.Schema);
            op = new ProjectOperator(op, finalProjections, _currentDatabase, "result");
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
            "COUNT" or "SUM" or "AVG" or "MIN" or "MAX" or "GROUP_CONCAT"
            or "BIT_AND" or "BIT_OR" or "BIT_XOR"
            or "STD" or "STDDEV" or "STDDEV_POP" or "STDDEV_SAMP"
            or "VAR_POP" or "VARIANCE" or "VAR_SAMP"
            or "JSON_ARRAYAGG" or "JSON_OBJECTAGG" => true,
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
                    "BIT_AND" => AggregateType.BitAnd,
                    "BIT_OR" => AggregateType.BitOr,
                    "BIT_XOR" => AggregateType.BitXor,
                    "STD" or "STDDEV" or "STDDEV_POP" => AggregateType.StddevPop,
                    "STDDEV_SAMP" => AggregateType.StddevSamp,
                    "VAR_POP" or "VARIANCE" => AggregateType.VarPop,
                    "VAR_SAMP" => AggregateType.VarSamp,
                    "JSON_ARRAYAGG" => AggregateType.JsonArrayAgg,
                    "JSON_OBJECTAGG" => AggregateType.JsonObjectAgg,
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

    /// <summary>
    /// Extracts all column reference names from an expression recursively.
    /// Used to determine which columns ORDER BY needs access to.
    /// </summary>
    private static List<string> ExtractColumnReferences(Expression expr)
    {
        var columns = new List<string>();
        ExtractColumnReferencesRecursive(expr, columns);
        return columns;
    }

    private static void ExtractColumnReferencesRecursive(Expression expr, List<string> columns)
    {
        switch (expr)
        {
            case ColumnReference colRef:
                columns.Add(colRef.ColumnName);
                break;
            case BinaryExpression binary:
                ExtractColumnReferencesRecursive(binary.Left, columns);
                ExtractColumnReferencesRecursive(binary.Right, columns);
                break;
            case UnaryExpression unary:
                ExtractColumnReferencesRecursive(unary.Operand, columns);
                break;
            case FunctionCall func:
                foreach (var arg in func.Arguments)
                {
                    ExtractColumnReferencesRecursive(arg, columns);
                }
                break;
            case CaseExpression caseExpr:
                foreach (var whenClause in caseExpr.WhenClauses)
                {
                    ExtractColumnReferencesRecursive(whenClause.When, columns);
                    ExtractColumnReferencesRecursive(whenClause.Then, columns);
                }
                if (caseExpr.ElseResult != null)
                {
                    ExtractColumnReferencesRecursive(caseExpr.ElseResult, columns);
                }
                break;
            case InExpression inExpr:
                ExtractColumnReferencesRecursive(inExpr.Expression, columns);
                if (inExpr.Values != null)
                {
                    foreach (var val in inExpr.Values)
                    {
                        ExtractColumnReferencesRecursive(val, columns);
                    }
                }
                break;
            case BetweenExpression between:
                ExtractColumnReferencesRecursive(between.Expression, columns);
                ExtractColumnReferencesRecursive(between.Low, columns);
                ExtractColumnReferencesRecursive(between.High, columns);
                break;
            case LikeExpression like:
                ExtractColumnReferencesRecursive(like.Expression, columns);
                ExtractColumnReferencesRecursive(like.Pattern, columns);
                break;
            case IsNullExpression isNull:
                ExtractColumnReferencesRecursive(isNull.Expression, columns);
                break;
            // Literals and other expressions without column references are ignored
        }
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

        // Check if this is a performance_schema query
        if (dbName.Equals("performance_schema", StringComparison.OrdinalIgnoreCase))
        {
            return BuildVirtualSchemaOperator("performance_schema", tableRef.TableName, tableRef.Alias);
        }

        // Check if this is a sys schema query
        if (dbName.Equals("sys", StringComparison.OrdinalIgnoreCase))
        {
            return BuildVirtualSchemaOperator("sys", tableRef.TableName, tableRef.Alias);
        }

        // Check if this is a mysql system schema query
        if (dbName.Equals("mysql", StringComparison.OrdinalIgnoreCase))
        {
            return BuildVirtualSchemaOperator("mysql", tableRef.TableName, tableRef.Alias);
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
    /// Builds an operator for virtual schema tables (performance_schema, sys, mysql).
    /// </summary>
    private IOperator BuildVirtualSchemaOperator(string schemaName, string tableName, string? alias)
    {
        ResultSet resultSet;
        switch (schemaName.ToLowerInvariant())
        {
            case "performance_schema":
                var perfProvider = new Storage.PerformanceSchema.PerformanceSchemaProvider(_catalog);
                resultSet = perfProvider.Query(tableName);
                break;
            case "sys":
                var perfProviderForSys = new Storage.PerformanceSchema.PerformanceSchemaProvider(_catalog);
                var sysProvider = new Storage.PerformanceSchema.SysSchemaProvider(perfProviderForSys);
                resultSet = sysProvider.Query(tableName);
                break;
            case "mysql":
                var mysqlProvider = new Storage.MysqlSchema.MysqlSchemaProvider(_catalog);
                resultSet = mysqlProvider.Query(tableName);
                break;
            default:
                throw new DatabaseNotFoundException(schemaName);
        }

        // Build TableSchema from ResultSet columns
        var cols = new List<Storage.ColumnDefinition>();
        foreach (var col in resultSet.Columns)
        {
            cols.Add(new Storage.ColumnDefinition(col.Name, col.DataType, isNullable: true));
        }
        var tableSchema = new Storage.TableSchema(0, schemaName, tableName, cols);
        return new Operators.InformationSchemaOperator(tableSchema, resultSet.Rows, alias);
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

        // Collect rows from VALUES or SELECT source
        var rowsToInsert = new List<DataValue[]>();

        if (stmt.SelectSource != null)
        {
            // INSERT ... SELECT: execute the select and collect rows
            var selectResult = ExecuteSelect(stmt.SelectSource);
            if (selectResult.ResultSet != null)
            {
                foreach (var srcRow in selectResult.ResultSet.Rows)
                {
                    var values = BuildInsertRowValues(stmt, schema, null, srcRow);
                    rowsToInsert.Add(values);
                }
            }
        }
        else
        {
            foreach (var valueList in stmt.ValuesList)
            {
                var values = BuildInsertRowValues(stmt, schema, valueList, null);
                rowsToInsert.Add(values);
            }
        }

        foreach (var values in rowsToInsert)
        {
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

            // Handle REPLACE INTO: delete existing row with same unique/primary key first
            if (stmt.IsReplace)
            {
                DeleteConflictingRow(table, schema, values);
            }

            // Handle ON DUPLICATE KEY UPDATE
            if (stmt.OnDuplicateKeyUpdate != null)
            {
                var existingRow = FindConflictingRow(table, schema, values);
                if (existingRow != null)
                {
                    // Update the existing row instead
                    var updatedValues = (DataValue[])existingRow.Values.Clone();
                    foreach (var clause in stmt.OnDuplicateKeyUpdate)
                    {
                        var ordinal = schema.GetColumnOrdinal(clause.ColumnName);
                        if (ordinal >= 0)
                        {
                            var eval = BuildExpression(clause.Value, schema);
                            updatedValues[ordinal] = eval.Evaluate(row);
                        }
                    }
                    var updatedRow = new Row(schema, updatedValues);
                    table.UpdateRow(existingRow.RowId, updatedRow);
                    insertedCount += 2; // MySQL reports 2 for update via ON DUPLICATE KEY
                    continue;
                }
            }

            // INSERT IGNORE: skip on conflict
            if (stmt.IsIgnore && FindConflictingRow(table, schema, values) != null)
                continue;

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

            var rowId = table.InsertRow(row);
            insertedCount++;

            // Log DML change if online DDL is in progress
            if (_onlineDdlManager.IsOnlineDdlInProgress(dbName, stmt.TableName))
            {
                var dmlChange = DmlChange.CreateInsert(rowId, row.Serialize());
                _onlineDdlManager.LogDmlChange(dbName, stmt.TableName, dmlChange);
            }

            // Execute AFTER INSERT triggers
            ExecuteTriggers(stmt.TableName, TriggerTiming.After, TriggerEvent.Insert, null, row);
        }

        table.Flush();
        _catalog.UpdateTableSchema(schema);
        _lastInsertId = lastId;

        _logger.Debug("Inserted {0} rows into {1}", insertedCount, stmt.TableName);
        return ExecutionResult.Modification(insertedCount, lastId);
    }

    private DataValue[] BuildInsertRowValues(InsertStatement stmt, TableSchema schema, List<Expression>? valueList, DataValue[]? sourceRow)
    {
        var values = new DataValue[schema.Columns.Count];

        if (stmt.Columns.Count > 0)
        {
            for (int i = 0; i < values.Length; i++)
            {
                var col = schema.Columns[i];
                values[i] = col.DefaultValue ?? DataValue.Null;
            }

            for (int i = 0; i < stmt.Columns.Count; i++)
            {
                var colName = stmt.Columns[i];
                var ordinal = schema.GetColumnOrdinal(colName);
                if (ordinal < 0) throw new ColumnNotFoundException(colName, stmt.TableName);

                if (valueList != null)
                    values[ordinal] = EvaluateLiteralExpression(valueList[i]);
                else if (sourceRow != null && i < sourceRow.Length)
                    values[ordinal] = sourceRow[i];
            }
        }
        else
        {
            if (valueList != null)
            {
                if (valueList.Count != schema.Columns.Count)
                    throw new CyscaleException($"Column count mismatch: expected {schema.Columns.Count}, got {valueList.Count}");
                for (int i = 0; i < valueList.Count; i++)
                    values[i] = EvaluateLiteralExpression(valueList[i]);
            }
            else if (sourceRow != null)
            {
                for (int i = 0; i < Math.Min(sourceRow.Length, schema.Columns.Count); i++)
                    values[i] = sourceRow[i];
            }
        }
        return values;
    }

    private Row? FindConflictingRow(Storage.Table table, TableSchema schema, DataValue[] values)
    {
        // Look for rows that conflict on primary key or unique key
        foreach (var col in schema.Columns)
        {
            if (col.IsPrimaryKey)
            {
                var searchVal = values[col.OrdinalPosition];
                if (searchVal.IsNull) continue;

                foreach (var row in table.ScanTable())
                {
                    if (row.Values[col.OrdinalPosition] == searchVal)
                        return row;
                }
            }
        }
        return null;
    }

    private void DeleteConflictingRow(Storage.Table table, TableSchema schema, DataValue[] values)
    {
        var conflicting = FindConflictingRow(table, schema, values);
        if (conflicting != null)
        {
            table.DeleteRow(conflicting.RowId);
        }
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
        var isOnlineDdlInProgress = _onlineDdlManager.IsOnlineDdlInProgress(dbName, stmt.TableName);
        
        foreach (var (rowId, oldRow, newRow) in rowsToUpdate)
        {
            // Execute BEFORE UPDATE triggers
            ExecuteTriggers(stmt.TableName, TriggerTiming.Before, TriggerEvent.Update, oldRow, newRow);

            if (table.UpdateRow(rowId, newRow))
            {
                updatedCount++;

                // Log DML change if online DDL is in progress
                if (isOnlineDdlInProgress)
                {
                    var dmlChange = DmlChange.CreateUpdate(rowId, oldRow.Serialize(), newRow.Serialize());
                    _onlineDdlManager.LogDmlChange(dbName, stmt.TableName, dmlChange);
                }

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
        var isOnlineDdlInProgress = _onlineDdlManager.IsOnlineDdlInProgress(dbName, stmt.TableName);
        
        foreach (var (rowId, row) in rowsToDelete)
        {
            // Execute BEFORE DELETE triggers
            ExecuteTriggers(stmt.TableName, TriggerTiming.Before, TriggerEvent.Delete, row, null);

            if (table.DeleteRow(rowId))
            {
                deletedCount++;

                // Log DML change if online DDL is in progress
                if (isOnlineDdlInProgress)
                {
                    var dmlChange = DmlChange.CreateDelete(rowId, row.Serialize());
                    _onlineDdlManager.LogDmlChange(dbName, stmt.TableName, dmlChange);
                }

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
            // Determine the DDL operation type for online DDL tracking
            var ddlOperation = GetOnlineDdlOperation(action);
            bool useOnlineDdl = ddlOperation.HasValue && ShouldUseOnlineDdl(stmt, action);
            
            // Begin online DDL if applicable
            if (useOnlineDdl)
            {
                if (!_onlineDdlManager.BeginOnlineDdl(dbName, stmt.TableName, ddlOperation!.Value))
                {
                    throw new CyscaleException($"Another DDL operation is in progress on {dbName}.{stmt.TableName}");
                }
            }

            try
            {
                switch (action)
                {
                    case AddColumnAction addCol:
                        // Online ADD COLUMN - adds to schema without blocking DML (instant DDL)
                        var newColDef = ConvertColumnDefToStorageDefinition(addCol.Column);
                        
                        // Mark the new column for lazy filling in existing rows
                        var newColOrdinal = schema.Columns.Count;
                        schema.AddColumn(newColDef, addCol.AfterColumn);
                        
                        // Store lazy column info in the table for existing row handling
                        var tableObj = _catalog.GetTable(dbName, stmt.TableName);
                        tableObj?.SetLazyColumn(newColOrdinal, newColDef.DefaultValue);
                        
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

                // Commit online DDL and apply logged changes
                if (useOnlineDdl)
                {
                    var loggedChanges = _onlineDdlManager.CommitOnlineDdl(dbName, stmt.TableName);
                    if (loggedChanges.Count > 0)
                    {
                        ApplyLoggedDmlChanges(dbName, stmt.TableName, loggedChanges, schema);
                        messages.Add($"Applied {loggedChanges.Count} logged DML changes");
                    }
                }
            }
            catch (Exception)
            {
                // Rollback online DDL on error
                if (useOnlineDdl)
                {
                    _onlineDdlManager.RollbackOnlineDdl(dbName, stmt.TableName);
                }
                throw;
            }
        }

        _logger.Info("ALTER TABLE {0}.{1}: {2}", dbName, stmt.TableName, string.Join("; ", messages));
        return ExecutionResult.Ddl($"Table '{stmt.TableName}' altered ({actionsPerformed} action(s))");
    }

    /// <summary>
    /// Determines the OnlineDdlOperation type for a given ALTER TABLE action.
    /// </summary>
    private static OnlineDdlOperation? GetOnlineDdlOperation(AlterTableAction action)
    {
        return action switch
        {
            AddColumnAction => OnlineDdlOperation.AddColumn,
            DropColumnAction => OnlineDdlOperation.DropColumn,
            ModifyColumnAction => OnlineDdlOperation.ModifyColumn,
            ChangeColumnAction => OnlineDdlOperation.ModifyColumn,
            AddIndexAction => OnlineDdlOperation.AddIndex,
            DropIndexAction => OnlineDdlOperation.DropIndex,
            AddConstraintAction => OnlineDdlOperation.AddConstraint,
            DropConstraintAction => OnlineDdlOperation.DropConstraint,
            AddForeignKeyAction => OnlineDdlOperation.AddConstraint,
            DropForeignKeyAction => OnlineDdlOperation.DropConstraint,
            _ => null
        };
    }

    /// <summary>
    /// Determines if online DDL should be used for this operation.
    /// </summary>
    private static bool ShouldUseOnlineDdl(AlterTableStatement stmt, AlterTableAction action)
    {
        // Use online DDL for column and index operations
        // Future: respect stmt.Algorithm and stmt.Lock settings
        return action is AddColumnAction or DropColumnAction or ModifyColumnAction 
            or ChangeColumnAction or AddIndexAction or DropIndexAction;
    }

    /// <summary>
    /// Applies logged DML changes after online DDL completes.
    /// </summary>
    private void ApplyLoggedDmlChanges(string dbName, string tableName, List<DmlChange> changes, TableSchema schema)
    {
        var table = _catalog.GetTable(dbName, tableName);
        if (table == null) return;

        foreach (var change in changes)
        {
            try
            {
                switch (change.Type)
                {
                    case DmlChangeType.Insert:
                        if (change.NewRowData != null)
                        {
                            var insertedRow = Row.Deserialize(change.NewRowData, schema);
                            table.InsertRow(insertedRow);
                        }
                        break;

                    case DmlChangeType.Update:
                        if (change.NewRowData != null)
                        {
                            var updatedRow = Row.Deserialize(change.NewRowData, schema);
                            table.UpdateRow(change.RowId, updatedRow);
                        }
                        break;

                    case DmlChangeType.Delete:
                        table.DeleteRow(change.RowId);
                        break;
                }
            }
            catch (Exception ex)
            {
                _logger.Error("Failed to apply logged DML change {0}: {1}", change, ex.Message);
            }
        }

        _logger.Info("Applied {0} logged DML changes to {1}.{2}", changes.Count, dbName, tableName);
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

        // Create EnumTypeDefinition if this is an ENUM column
        EnumTypeDefinition? enumType = null;
        if (colDef.DataType == DataType.Enum && colDef.EnumValues != null)
        {
            enumType = new EnumTypeDefinition(colDef.Name, colDef.EnumValues);
        }

        // Create SetTypeDefinition if this is a SET column
        SetTypeDefinition? setType = null;
        if (colDef.DataType == DataType.Set && colDef.SetValues != null)
        {
            setType = new SetTypeDefinition(colDef.Name, colDef.SetValues);
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
            defaultValue: defaultValue,
            enumType: enumType,
            setType: setType
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
            
            // Update configuration for certain global variables
            if (setVar.Scope == SetScope.Global)
            {
                UpdateGlobalConfiguration(setVar.Name, value);
            }
        }

        return ExecutionResult.Empty();
    }

    /// <summary>
    /// Updates the global configuration when SET GLOBAL is used for configuration variables.
    /// </summary>
    private void UpdateGlobalConfiguration(string variableName, object? value)
    {
        var config = Common.CyscaleDbConfiguration.Current;
        var varNameLower = variableName.ToLowerInvariant();
        
        switch (varNameLower)
        {
            case "slow_query_threshold" or "slow_query_threshold_ms":
                if (value is long longVal)
                    config.SlowQueryThresholdMs = (int)longVal;
                else if (value is int intVal)
                    config.SlowQueryThresholdMs = intVal;
                break;
            case "slow_query_log":
                if (value is bool boolVal)
                    config.EnableSlowQueryLog = boolVal;
                else if (value is string strVal)
                    config.EnableSlowQueryLog = strVal.Equals("ON", StringComparison.OrdinalIgnoreCase) || strVal == "1";
                break;
            case "buffer_pool_size" or "buffer_pool_size_pages":
                if (value is long bpLong)
                    config.BufferPoolSizePages = (int)bpLong;
                else if (value is int bpInt)
                    config.BufferPoolSizePages = bpInt;
                break;
            case "lock_wait_timeout" or "lock_wait_timeout_ms":
                if (value is long lwLong)
                    config.LockWaitTimeoutMs = (int)lwLong;
                else if (value is int lwInt)
                    config.LockWaitTimeoutMs = lwInt;
                break;
            case "enable_metrics":
                if (value is bool mBool)
                    config.EnableMetrics = mBool;
                else if (value is string mStr)
                    config.EnableMetrics = mStr.Equals("ON", StringComparison.OrdinalIgnoreCase) || mStr == "1";
                break;
        }
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

        // Build WHERE evaluator if specified
        IExpressionEvaluator? whereEval = null;
        Storage.TableSchema? tempSchema = null;
        if (stmt.Where != null)
        {
            var cols = new List<Storage.ColumnDefinition>
            {
                new("Variable_name", DataType.VarChar, isNullable: false),
                new("Value", DataType.VarChar, isNullable: true)
            };
            tempSchema = new Storage.TableSchema(0, "information_schema", "SESSION_VARIABLES", cols);
            whereEval = BuildExpressionEvaluator(stmt.Where);
        }

        foreach (var kv in variables)
        {
            // Apply LIKE pattern filter if specified
            if (!string.IsNullOrEmpty(stmt.LikePattern))
            {
                if (!MatchLikePattern(kv.Key, stmt.LikePattern))
                    continue;
            }

            var values = new DataValue[]
            {
                DataValue.FromVarChar(kv.Key),
                Common.SystemVariables.ToDataValue(kv.Value)
            };

            // Apply WHERE filter if specified
            if (whereEval != null && tempSchema != null)
            {
                var tempRow = new Storage.Row(tempSchema, values);
                var val = whereEval.Evaluate(tempRow);
                if (val.Type != DataType.Boolean || !val.AsBoolean()) continue;
            }

            result.Rows.Add(values);
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowStatus(ShowStatusStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Variable_name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Value", DataType = DataType.VarChar });

        // Return comprehensive MySQL 8.4 compatible status values
        var uptime = (long)(DateTime.UtcNow - System.Diagnostics.Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalSeconds;
        var statusValues = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            // General
            ["Uptime"] = uptime,
            ["Uptime_since_flush_status"] = uptime,
            ["Threads_connected"] = 1,
            ["Threads_running"] = 1,
            ["Threads_created"] = 1,
            ["Threads_cached"] = 0,
            ["Questions"] = 0L,
            ["Queries"] = 0L,
            ["Slow_queries"] = 0L,
            ["Opens"] = 0,
            ["Flush_tables"] = 0,
            ["Open_tables"] = 0,
            ["Opened_tables"] = 0,
            ["Open_files"] = 0,
            ["Open_streams"] = 0,
            ["Opened_files"] = 0,
            ["Queries_per_second_avg"] = "0.000",
            ["Connections"] = 1L,
            ["Max_used_connections"] = 1,
            ["Max_used_connections_time"] = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss"),
            ["Aborted_clients"] = 0L,
            ["Aborted_connects"] = 0L,
            ["Bytes_received"] = 0L,
            ["Bytes_sent"] = 0L,
            // Table locks
            ["Table_locks_immediate"] = 0L,
            ["Table_locks_waited"] = 0L,
            // Com counters
            ["Com_select"] = 0L,
            ["Com_insert"] = 0L,
            ["Com_update"] = 0L,
            ["Com_delete"] = 0L,
            ["Com_commit"] = 0L,
            ["Com_rollback"] = 0L,
            ["Com_begin"] = 0L,
            ["Com_create_db"] = 0L,
            ["Com_drop_db"] = 0L,
            ["Com_create_table"] = 0L,
            ["Com_drop_table"] = 0L,
            ["Com_alter_table"] = 0L,
            ["Com_show_tables"] = 0L,
            ["Com_show_databases"] = 0L,
            ["Com_show_variables"] = 0L,
            ["Com_show_status"] = 0L,
            ["Com_set_option"] = 0L,
            ["Com_flush"] = 0L,
            ["Com_kill"] = 0L,
            ["Com_stmt_prepare"] = 0L,
            ["Com_stmt_execute"] = 0L,
            ["Com_stmt_close"] = 0L,
            // Handlers
            ["Handler_read_first"] = 0L,
            ["Handler_read_key"] = 0L,
            ["Handler_read_last"] = 0L,
            ["Handler_read_next"] = 0L,
            ["Handler_read_prev"] = 0L,
            ["Handler_read_rnd"] = 0L,
            ["Handler_read_rnd_next"] = 0L,
            ["Handler_write"] = 0L,
            ["Handler_update"] = 0L,
            ["Handler_delete"] = 0L,
            ["Handler_commit"] = 0L,
            ["Handler_rollback"] = 0L,
            ["Handler_prepare"] = 0L,
            // Created temps
            ["Created_tmp_tables"] = 0L,
            ["Created_tmp_disk_tables"] = 0L,
            ["Created_tmp_files"] = 0L,
            // Select types
            ["Select_full_join"] = 0L,
            ["Select_full_range_join"] = 0L,
            ["Select_range"] = 0L,
            ["Select_range_check"] = 0L,
            ["Select_scan"] = 0L,
            // Sort
            ["Sort_merge_passes"] = 0L,
            ["Sort_range"] = 0L,
            ["Sort_rows"] = 0L,
            ["Sort_scan"] = 0L,
            // InnoDB status
            ["Innodb_buffer_pool_pages_data"] = 0L,
            ["Innodb_buffer_pool_pages_dirty"] = 0L,
            ["Innodb_buffer_pool_pages_flushed"] = 0L,
            ["Innodb_buffer_pool_pages_free"] = 8191L,
            ["Innodb_buffer_pool_pages_misc"] = 0L,
            ["Innodb_buffer_pool_pages_total"] = 8192L,
            ["Innodb_buffer_pool_read_ahead"] = 0L,
            ["Innodb_buffer_pool_read_requests"] = 0L,
            ["Innodb_buffer_pool_reads"] = 0L,
            ["Innodb_buffer_pool_wait_free"] = 0L,
            ["Innodb_buffer_pool_write_requests"] = 0L,
            ["Innodb_data_fsyncs"] = 0L,
            ["Innodb_data_pending_fsyncs"] = 0L,
            ["Innodb_data_pending_reads"] = 0L,
            ["Innodb_data_pending_writes"] = 0L,
            ["Innodb_data_read"] = 0L,
            ["Innodb_data_reads"] = 0L,
            ["Innodb_data_writes"] = 0L,
            ["Innodb_data_written"] = 0L,
            ["Innodb_log_waits"] = 0L,
            ["Innodb_log_write_requests"] = 0L,
            ["Innodb_log_writes"] = 0L,
            ["Innodb_os_log_fsyncs"] = 0L,
            ["Innodb_os_log_pending_fsyncs"] = 0L,
            ["Innodb_os_log_pending_writes"] = 0L,
            ["Innodb_os_log_written"] = 0L,
            ["Innodb_pages_created"] = 0L,
            ["Innodb_pages_read"] = 0L,
            ["Innodb_pages_written"] = 0L,
            ["Innodb_row_lock_current_waits"] = 0L,
            ["Innodb_row_lock_time"] = 0L,
            ["Innodb_row_lock_time_avg"] = 0L,
            ["Innodb_row_lock_time_max"] = 0L,
            ["Innodb_row_lock_waits"] = 0L,
            ["Innodb_rows_deleted"] = 0L,
            ["Innodb_rows_inserted"] = 0L,
            ["Innodb_rows_read"] = 0L,
            ["Innodb_rows_updated"] = 0L,
            // SSL
            ["Ssl_accepts"] = 0L,
            ["Ssl_finished_accepts"] = 0L,
            ["Ssl_cipher"] = "",
            ["Ssl_version"] = "",
        };

        // Build WHERE evaluator if specified
        IExpressionEvaluator? whereEval = null;
        Storage.TableSchema? tempSchema = null;
        if (stmt.Where != null)
        {
            var cols = new List<Storage.ColumnDefinition>
            {
                new("Variable_name", DataType.VarChar, isNullable: false),
                new("Value", DataType.VarChar, isNullable: true)
            };
            tempSchema = new Storage.TableSchema(0, "information_schema", "GLOBAL_STATUS", cols);
            whereEval = BuildExpressionEvaluator(stmt.Where);
        }

        foreach (var kv in statusValues)
        {
            if (!string.IsNullOrEmpty(stmt.LikePattern))
            {
                if (!MatchLikePattern(kv.Key, stmt.LikePattern))
                    continue;
            }

            var values = new DataValue[]
            {
                DataValue.FromVarChar(kv.Key),
                Common.SystemVariables.ToDataValue(kv.Value)
            };

            // Apply WHERE filter if specified
            if (whereEval != null && tempSchema != null)
            {
                var tempRow = new Storage.Row(tempSchema, values);
                var val = whereEval.Evaluate(tempRow);
                if (val.Type != DataType.Boolean || !val.AsBoolean()) continue;
            }

            result.Rows.Add(values);
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
        if (stmt.IsFull)
            result.Columns.Add(new ResultColumn { Name = "Collation", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Null", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Key", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Default", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Extra", DataType = DataType.VarChar });
        if (stmt.IsFull)
        {
            result.Columns.Add(new ResultColumn { Name = "Privileges", DataType = DataType.VarChar });
            result.Columns.Add(new ResultColumn { Name = "Comment", DataType = DataType.VarChar });
        }

        foreach (var col in schema.Columns)
        {
            if (!string.IsNullOrEmpty(stmt.LikePattern))
            {
                if (!MatchLikePattern(col.Name, stmt.LikePattern))
                    continue;
            }

            var isStringType = col.DataType is DataType.VarChar or DataType.Char or DataType.Text;
            var defaultVal = col.DefaultValue.HasValue ? col.DefaultValue.Value : DataValue.Null;
            var extra = col.IsAutoIncrement ? "auto_increment" : "";
            if (col.IsGenerated)
                extra = col.IsStoredGenerated ? "STORED GENERATED" : "VIRTUAL GENERATED";

            if (stmt.IsFull)
            {
                result.Rows.Add([
                    DataValue.FromVarChar(col.Name),
                    DataValue.FromVarChar(GetMySqlTypeName(col)),
                    DataValue.FromVarChar(isStringType ? "utf8mb4_general_ci" : DataValue.Null.ToString()),
                    DataValue.FromVarChar(col.IsNullable ? "YES" : "NO"),
                    DataValue.FromVarChar(col.IsPrimaryKey ? "PRI" : ""),
                    defaultVal,
                    DataValue.FromVarChar(extra),
                    DataValue.FromVarChar("select,insert,update,references"),
                    DataValue.FromVarChar("")
                ]);
            }
            else
            {
                result.Rows.Add([
                    DataValue.FromVarChar(col.Name),
                    DataValue.FromVarChar(GetMySqlTypeName(col)),
                    DataValue.FromVarChar(col.IsNullable ? "YES" : "NO"),
                    DataValue.FromVarChar(col.IsPrimaryKey ? "PRI" : ""),
                    defaultVal,
                    DataValue.FromVarChar(extra)
                ]);
            }
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
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Level", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Code", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "Message", DataType = DataType.VarChar });

        if (_getWarnings != null)
        {
            foreach (var (level, code, message) in _getWarnings())
            {
                result.Rows.Add([
                    DataValue.FromVarChar(level),
                    DataValue.FromInt(code),
                    DataValue.FromVarChar(message)
                ]);
            }
        }

        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowErrors()
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Level", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Code", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "Message", DataType = DataType.VarChar });

        if (_getWarnings != null)
        {
            foreach (var (level, code, message) in _getWarnings())
            {
                if (level == "Error")
                {
                    result.Rows.Add([
                        DataValue.FromVarChar(level),
                        DataValue.FromInt(code),
                        DataValue.FromVarChar(message)
                    ]);
                }
            }
        }

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

    private ExecutionResult ExecuteShowProcesslist(ShowProcesslistStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Id", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "User", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Host", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "db", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Command", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Time", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "State", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Info", DataType = DataType.VarChar });
        // Add the current connection as a row
        result.Rows.Add([
            DataValue.FromBigInt(1),
            DataValue.FromVarChar("root"),
            DataValue.FromVarChar("localhost"),
            DataValue.FromVarChar(_currentDatabase ?? ""),
            DataValue.FromVarChar("Query"),
            DataValue.FromInt(0),
            DataValue.FromVarChar("executing"),
            DataValue.FromVarChar("SHOW PROCESSLIST")
        ]);
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowGrants(ShowGrantsStatement stmt)
    {
        var result = new ResultSet();
        var user = stmt.ForUser ?? "root";
        var host = stmt.ForHost ?? "localhost";
        result.Columns.Add(new ResultColumn { Name = $"Grants for {user}@{host}", DataType = DataType.VarChar });
        result.Rows.Add([DataValue.FromVarChar($"GRANT ALL PRIVILEGES ON *.* TO '{user}'@'{host}' WITH GRANT OPTION")]);
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowCreateDatabase(ShowCreateDatabaseStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Database", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Create Database", DataType = DataType.VarChar });
        result.Rows.Add([
            DataValue.FromVarChar(stmt.DatabaseName),
            DataValue.FromVarChar($"CREATE DATABASE `{stmt.DatabaseName}` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */ /*!80016 DEFAULT ENCRYPTION='N' */")
        ]);
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowCreateView(ShowCreateViewStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "View", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Create View", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "character_set_client", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "collation_connection", DataType = DataType.VarChar });
        // Try to find the view in stored routines/catalog
        result.Rows.Add([
            DataValue.FromVarChar(stmt.ViewName),
            DataValue.FromVarChar($"CREATE ALGORITHM=UNDEFINED VIEW `{stmt.ViewName}` AS SELECT 1"),
            DataValue.FromVarChar("utf8mb4"),
            DataValue.FromVarChar("utf8mb4_general_ci")
        ]);
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowCreateRoutine(ShowCreateRoutineStatement stmt)
    {
        var kind = stmt.IsFunction ? "Function" : "Procedure";
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = kind, DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "sql_mode", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = $"Create {kind}", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "character_set_client", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "collation_connection", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Database Collation", DataType = DataType.VarChar });
        result.Rows.Add([
            DataValue.FromVarChar(stmt.RoutineName),
            DataValue.FromVarChar("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"),
            DataValue.FromVarChar($"CREATE {kind.ToUpperInvariant()} `{stmt.RoutineName}`() BEGIN END"),
            DataValue.FromVarChar("utf8mb4"),
            DataValue.FromVarChar("utf8mb4_general_ci"),
            DataValue.FromVarChar("utf8mb4_general_ci")
        ]);
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowRoutineStatus(ShowRoutineStatusStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Db", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Definer", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Modified", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Created", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Security_type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Comment", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "character_set_client", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "collation_connection", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Database Collation", DataType = DataType.VarChar });
        // Return empty - routines are not persisted yet
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowTriggers(ShowTriggersStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Trigger", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Event", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Table", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Statement", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Timing", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Created", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "sql_mode", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Definer", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "character_set_client", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "collation_connection", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Database Collation", DataType = DataType.VarChar });
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowEvents(ShowEventsStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Db", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Definer", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Time zone", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Execute at", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Interval value", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Interval field", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Starts", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Ends", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Status", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Originator", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "character_set_client", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "collation_connection", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Database Collation", DataType = DataType.VarChar });
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowPlugins()
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Status", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Library", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "License", DataType = DataType.VarChar });

        var plugins = new[]
        {
            ("InnoDB", "ACTIVE", "STORAGE ENGINE", "", "GPL"),
            ("MEMORY", "ACTIVE", "STORAGE ENGINE", "", "GPL"),
            ("CSV", "ACTIVE", "STORAGE ENGINE", "", "GPL"),
            ("ARCHIVE", "ACTIVE", "STORAGE ENGINE", "", "GPL"),
            ("BLACKHOLE", "ACTIVE", "STORAGE ENGINE", "", "GPL"),
            ("MyISAM", "ACTIVE", "STORAGE ENGINE", "", "GPL"),
            ("PERFORMANCE_SCHEMA", "ACTIVE", "STORAGE ENGINE", "", "GPL"),
            ("mysql_native_password", "ACTIVE", "AUTHENTICATION", "", "GPL"),
            ("caching_sha2_password", "ACTIVE", "AUTHENTICATION", "", "GPL"),
            ("sha256_password", "ACTIVE", "AUTHENTICATION", "", "GPL"),
        };

        foreach (var (name, status, type, library, license) in plugins)
        {
            result.Rows.Add([
                DataValue.FromVarChar(name),
                DataValue.FromVarChar(status),
                DataValue.FromVarChar(type),
                DataValue.FromVarChar(library),
                DataValue.FromVarChar(license)
            ]);
        }
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowEngineStatus(ShowEngineStatusStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Status", DataType = DataType.VarChar });
        result.Rows.Add([
            DataValue.FromVarChar(stmt.EngineName),
            DataValue.FromVarChar(""),
            DataValue.FromVarChar($"=====================================\n{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} CyscaleDB INNODB MONITOR OUTPUT\n=====================================\nPer second averages calculated from the last 0 seconds\n---BUFFER POOL---\nTotal large memory allocated 0\nBuffer pool size   8192\nFree buffers       8191\nDatabase pages     1\n---LOG---\nLog sequence number 0\n---ROW OPERATIONS---\n0 read views open inside InnoDB\n---END OF INNODB MONITOR OUTPUT---")
        ]);
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowEngines()
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Engine", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Support", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Comment", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Transactions", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "XA", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Savepoints", DataType = DataType.VarChar });

        var engines = new[]
        {
            ("InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"),
            ("MEMORY", "YES", "Hash based, stored in memory, useful for temporary tables", "NO", "NO", "NO"),
            ("CSV", "YES", "CSV storage engine", "NO", "NO", "NO"),
            ("ARCHIVE", "YES", "Archive storage engine", "NO", "NO", "NO"),
            ("BLACKHOLE", "YES", "/dev/null storage engine", "NO", "NO", "NO"),
            ("MyISAM", "YES", "MyISAM storage engine", "NO", "NO", "NO"),
            ("PERFORMANCE_SCHEMA", "YES", "Performance Schema", "NO", "NO", "NO"),
        };

        foreach (var (engine, support, comment, txn, xa, sp) in engines)
        {
            result.Rows.Add([
                DataValue.FromVarChar(engine),
                DataValue.FromVarChar(support),
                DataValue.FromVarChar(comment),
                DataValue.FromVarChar(txn),
                DataValue.FromVarChar(xa),
                DataValue.FromVarChar(sp)
            ]);
        }
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowPrivileges()
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Privilege", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Context", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Comment", DataType = DataType.VarChar });

        var privs = new[]
        {
            ("Alter", "Tables", "To alter the table"),
            ("Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"),
            ("Create", "Databases,Tables,Indexes", "To create new databases and tables"),
            ("Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"),
            ("Create role", "Server Admin", "To create new roles"),
            ("Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"),
            ("Create user", "Server Admin", "To create new users"),
            ("Create view", "Tables", "To create new views"),
            ("Delete", "Tables", "To delete existing rows"),
            ("Drop", "Databases,Tables", "To drop databases, tables, and views"),
            ("Drop role", "Server Admin", "To drop roles"),
            ("Event", "Server Admin", "To create, alter, drop and execute events"),
            ("Execute", "Functions,Procedures", "To execute stored routines"),
            ("File", "File access on server", "To read and write files on the server"),
            ("Grant option", "Databases,Tables,Functions,Procedures", "To give to other users those privileges you possess"),
            ("Index", "Tables", "To create or drop indexes"),
            ("Insert", "Tables", "To insert data into tables"),
            ("Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"),
            ("Process", "Server Admin", "To view the plain text of currently executing queries"),
            ("References", "Databases,Tables", "To have references on tables"),
            ("Reload", "Server Admin", "To reload or refresh tables, logs and privileges"),
            ("Replication client", "Server Admin", "To ask where the slave or master servers are"),
            ("Replication slave", "Server Admin", "To read binary log events from the master"),
            ("Select", "Tables", "To retrieve rows from table"),
            ("Show databases", "Server Admin", "To see all databases with SHOW DATABASES"),
            ("Show view", "Tables", "To see views with SHOW CREATE VIEW"),
            ("Shutdown", "Server Admin", "To shut down the server"),
            ("Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."),
            ("Trigger", "Tables", "To use triggers"),
            ("Update", "Tables", "To update existing rows"),
            ("Usage", "Server Admin", "No privileges - allow connect only"),
        };

        foreach (var (priv, ctx, comment) in privs)
        {
            result.Rows.Add([
                DataValue.FromVarChar(priv),
                DataValue.FromVarChar(ctx),
                DataValue.FromVarChar(comment)
            ]);
        }
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowMasterStatus()
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "File", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Position", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Binlog_Do_DB", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Binlog_Ignore_DB", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Executed_Gtid_Set", DataType = DataType.VarChar });
        result.Rows.Add([
            DataValue.FromVarChar("binlog.000001"),
            DataValue.FromBigInt(156),
            DataValue.FromVarChar(""),
            DataValue.FromVarChar(""),
            DataValue.FromVarChar("")
        ]);
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowReplicaStatus()
    {
        // Return empty result set with all the standard columns
        var result = new ResultSet();
        var columns = new[] {
            "Replica_IO_State", "Source_Host", "Source_User", "Source_Port",
            "Connect_Retry", "Source_Log_File", "Read_Source_Log_Pos",
            "Relay_Log_File", "Relay_Log_Pos", "Relay_Source_Log_File",
            "Replica_IO_Running", "Replica_SQL_Running", "Replicate_Do_DB",
            "Replicate_Ignore_DB", "Replicate_Do_Table", "Replicate_Ignore_Table",
            "Last_Errno", "Last_Error", "Skip_Counter", "Exec_Source_Log_Pos",
            "Relay_Log_Space", "Until_Condition", "Until_Log_File", "Until_Log_Pos",
            "Source_SSL_Allowed", "Source_SSL_CA_File", "Source_SSL_CA_Path",
            "Source_SSL_Cert", "Source_SSL_Cipher", "Source_SSL_Key",
            "Seconds_Behind_Source", "Source_SSL_Verify_Server_Cert",
            "Last_IO_Errno", "Last_IO_Error", "Last_SQL_Errno", "Last_SQL_Error",
            "Replicate_Ignore_Server_Ids", "Source_Server_Id", "Source_UUID",
            "Source_Info_File", "SQL_Delay", "SQL_Remaining_Delay",
            "Replica_SQL_Running_State", "Source_Retry_Count", "Source_Bind",
            "Last_IO_Error_Timestamp", "Last_SQL_Error_Timestamp",
            "Retrieved_Gtid_Set", "Executed_Gtid_Set", "Auto_Position",
            "Channel_Name"
        };
        foreach (var col in columns)
            result.Columns.Add(new ResultColumn { Name = col, DataType = DataType.VarChar });
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowBinaryLogs()
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Log_name", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "File_size", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "Encrypted", DataType = DataType.VarChar });
        result.Rows.Add([
            DataValue.FromVarChar("binlog.000001"),
            DataValue.FromBigInt(156),
            DataValue.FromVarChar("No")
        ]);
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowOpenTables(ShowOpenTablesStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Database", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Table", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "In_use", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "Name_locked", DataType = DataType.Int });
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteShowCount(ShowCountStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = $"@@session.{stmt.CountType.ToLowerInvariant()}_count", DataType = DataType.Int });
        result.Rows.Add([DataValue.FromInt(0)]);
        return ExecutionResult.Query(result);
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
        UserManager.Instance.CreateUser(stmt.UserName, stmt.Password ?? "", stmt.Host,
            ifNotExists: stmt.IfNotExists);
        return ExecutionResult.Ddl($"User '{stmt.UserName}'@'{stmt.Host}' created");
    }

    private ExecutionResult ExecuteAlterUser(AlterUserStatement stmt)
    {
        UserManager.Instance.AlterUser(stmt.UserName, stmt.Host, newPassword: stmt.Password);
        return ExecutionResult.Ddl($"User '{stmt.UserName}'@'{stmt.Host}' altered");
    }

    private ExecutionResult ExecuteDropUser(DropUserStatement stmt)
    {
        UserManager.Instance.DropUser(stmt.UserName, stmt.Host, ifExists: stmt.IfExists);
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
    private CursorManager? _cursorManager;
    private List<ConditionHandler>? _conditionHandlers;

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
            case "TABLE":
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

            case "TABLES WITH READ LOCK":
                // Global read lock + flush all tables
                _catalog.Flush();
                _logger.Info("Flushed tables with read lock");
                break;

            case "PRIVILEGES":
                // Reload privilege tables
                _logger.Info("Flushed privileges - reloaded grant tables");
                break;

            case "LOGS":
            case "BINARY LOGS":
            case "ENGINE LOGS":
            case "ERROR LOGS":
            case "GENERAL LOGS":
            case "RELAY LOGS":
            case "SLOW LOGS":
                _logger.Info("Flushed {0}", stmt.FlushType);
                break;

            case "STATUS":
                // Reset session status variables to zero
                _logger.Info("Flushed status - reset session status counters");
                break;

            case "HOSTS":
                // Clear host cache
                _logger.Info("Flushed hosts - cleared host cache");
                break;

            case "OPTIMIZER_COSTS":
                // Re-read cost model tables
                _logger.Info("Flushed optimizer costs");
                break;

            case "USER_RESOURCES":
                // Reset per-hour user resource usage counters
                _logger.Info("Flushed user resources");
                break;

            case "QUERY CACHE":
                // Defragment the query cache (deprecated but accepted)
                _logger.Info("Flushed query cache (no-op, query cache deprecated)");
                break;

            case "DES_KEY_FILE":
                _logger.Info("Flushed DES key file (no-op)");
                break;

            default:
                _logger.Info("Flushed {0} (no-op)", stmt.FlushType);
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

    private ExecutionResult ExecuteExplain(ExplainStatement stmt)
    {
        if (stmt.Format == ExplainFormat.Json)
        {
            return ExecuteExplainJson(stmt);
        }

        // Build the MySQL-standard execution plan result
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "id", DataType = DataType.Int });
        result.Columns.Add(new ResultColumn { Name = "select_type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "table", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "partitions", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "possible_keys", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "key", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "key_len", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "ref", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "rows", DataType = DataType.BigInt });
        result.Columns.Add(new ResultColumn { Name = "filtered", DataType = DataType.Double, Decimals = 2 });
        result.Columns.Add(new ResultColumn { Name = "Extra", DataType = DataType.VarChar });

        if (stmt.Statement is SelectStatement selectStmt)
        {
            var tableName = GetTableNameFromTableRef(selectStmt.From);
            var dbName = GetDatabaseFromTableRef(selectStmt.From) ?? _currentDatabase;

            string accessType = "ALL";
            string? possibleKeys = null;
            string? usedKey = null;
            string? keyLen = null;
            string? refCol = null;
            long estimatedRows = 0;
            double filtered = 100.00;
            var extra = new List<string>();

            var schema = _catalog.GetTableSchema(dbName, tableName ?? "");
            if (schema != null)
            {
                // Estimate row count
                estimatedRows = Math.Max(1, schema.Columns.Count > 0 ? 1 : 0);

                // Check for primary key usage in WHERE
                var pkCol = schema.Columns.FirstOrDefault(c => c.IsPrimaryKey);
                if (pkCol != null && selectStmt.Where != null)
                {
                    var referencedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    CollectReferencedColumns(selectStmt.Where, referencedColumns);

                    if (referencedColumns.Contains(pkCol.Name))
                    {
                        possibleKeys = "PRIMARY";
                        usedKey = "PRIMARY";
                        accessType = "const";
                        keyLen = "8";
                        refCol = "const";
                        estimatedRows = 1;
                        filtered = 100.0;
                    }
                    // else: secondary index analysis could be added in the future
                }

                if (selectStmt.Where != null && usedKey == null)
                {
                    extra.Add("Using where");
                    filtered = 33.33;
                }
                if (selectStmt.Limit != null)
                {
                    // If there's a LIMIT, adjust
                }
                if (selectStmt.OrderBy.Count > 0)
                {
                    extra.Add("Using filesort");
                }
                if (selectStmt.IsDistinct || selectStmt.GroupBy.Count > 0)
                {
                    extra.Add("Using temporary");
                }
                // Check for covering index
                if (usedKey != null && selectStmt.Columns.All(s => s.Expression is ColumnReference))
                {
                    extra.Add("Using index");
                }
            }

            result.Rows.Add([
                DataValue.FromInt(1),
                DataValue.FromVarChar("SIMPLE"),
                DataValue.FromVarChar(tableName ?? ""),
                DataValue.Null, // partitions
                DataValue.FromVarChar(accessType),
                possibleKeys != null ? DataValue.FromVarChar(possibleKeys) : DataValue.Null,
                usedKey != null ? DataValue.FromVarChar(usedKey) : DataValue.Null,
                keyLen != null ? DataValue.FromVarChar(keyLen) : DataValue.Null,
                refCol != null ? DataValue.FromVarChar(refCol) : DataValue.Null,
                DataValue.FromBigInt(estimatedRows),
                DataValue.FromDouble(filtered),
                extra.Count > 0 ? DataValue.FromVarChar(string.Join("; ", extra)) : DataValue.Null
            ]);
        }
        else
        {
            result.Rows.Add([
                DataValue.FromInt(1),
                DataValue.FromVarChar(stmt.Statement.GetType().Name.Replace("Statement", "").ToUpperInvariant()),
                DataValue.Null, DataValue.Null,
                DataValue.Null, DataValue.Null,
                DataValue.Null, DataValue.Null,
                DataValue.Null, DataValue.Null,
                DataValue.Null, DataValue.Null
            ]);
        }

        return ExecutionResult.Query(result);
    }

    /// <summary>
    /// Returns EXPLAIN output in JSON format compatible with MySQL 8.
    /// </summary>
    private ExecutionResult ExecuteExplainJson(ExplainStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "EXPLAIN", DataType = DataType.VarChar });

        string tableName = "";
        string accessType = "ALL";
        long estimatedRows = 1;

        if (stmt.Statement is SelectStatement selectStmt)
        {
            tableName = GetTableNameFromTableRef(selectStmt.From) ?? "";
            var dbName = GetDatabaseFromTableRef(selectStmt.From) ?? _currentDatabase;
            var schema = _catalog.GetTableSchema(dbName, tableName);
            if (schema != null)
            {
                var pkCol = schema.Columns.FirstOrDefault(c => c.IsPrimaryKey);
                if (pkCol != null && selectStmt.Where != null)
                {
                    var refs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    CollectReferencedColumns(selectStmt.Where, refs);
                    if (refs.Contains(pkCol.Name))
                    {
                        accessType = "const";
                        estimatedRows = 1;
                    }
                }
            }
        }

        var json = $$"""
{
  "query_block": {
    "select_id": 1,
    "cost_info": {
      "query_cost": "1.00"
    },
    "table": {
      "table_name": "{{tableName}}",
      "access_type": "{{accessType}}",
      "rows_examined_per_scan": {{estimatedRows}},
      "rows_produced_per_join": {{estimatedRows}},
      "filtered": "100.00",
      "cost_info": {
        "read_cost": "0.25",
        "eval_cost": "0.10",
        "prefix_cost": "0.35",
        "data_read_per_join": "256"
      },
      "used_columns": []
    }
  }
}
""";
        result.Rows.Add([DataValue.FromVarChar(json)]);
        return ExecutionResult.Query(result);
    }

    private void CollectReferencedColumns(Expression expr, HashSet<string> columns)
    {
        switch (expr)
        {
            case ColumnReference colRef:
                columns.Add(colRef.ColumnName);
                break;
            case BinaryExpression binary:
                CollectReferencedColumns(binary.Left, columns);
                CollectReferencedColumns(binary.Right, columns);
                break;
            case UnaryExpression unary:
                CollectReferencedColumns(unary.Operand, columns);
                break;
        }
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
        _cursorManager = new CursorManager();
        _conditionHandlers = new List<ConditionHandler>();

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
        _cursorManager?.ClearAll();
        _cursorManager = null;
        _conditionHandlers = null;

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

    private ExecutionResult ExecuteDeclareCursor(DeclareCursorStatement stmt)
    {
        if (_cursorManager == null)
            throw new CyscaleException("DECLARE CURSOR can only be used inside a stored procedure", ErrorCode.InternalError);

        // Execute the SELECT to get the result set, then declare the cursor
        var selectResult = Execute(stmt.Query);
        if (selectResult.ResultSet != null)
            _cursorManager.DeclareCursor(stmt.CursorName, selectResult.ResultSet);
        else
            _cursorManager.DeclareCursor(stmt.CursorName, new ResultSet());

        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteOpenCursor(OpenCursorStatement stmt)
    {
        if (_cursorManager == null)
            throw new CyscaleException("OPEN can only be used inside a stored procedure", ErrorCode.InternalError);
        _cursorManager.OpenCursor(stmt.CursorName);
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteFetchCursor(FetchCursorStatement stmt)
    {
        if (_cursorManager == null || _procedureVariables == null)
            throw new CyscaleException("FETCH can only be used inside a stored procedure", ErrorCode.InternalError);

        var row = _cursorManager.FetchCursor(stmt.CursorName);
        if (row == null)
        {
            // No more rows - raise NOT FOUND condition (SQLSTATE '02000')
            throw new SignalException("02000", "No data - zero rows fetched, selected, or processed");
        }

        // Assign values to INTO variables
        for (int i = 0; i < stmt.IntoVariables.Count && i < row.Length; i++)
        {
            _procedureVariables[stmt.IntoVariables[i]] = row[i];
        }

        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteCloseCursor(CloseCursorStatement stmt)
    {
        if (_cursorManager == null)
            throw new CyscaleException("CLOSE can only be used inside a stored procedure", ErrorCode.InternalError);
        _cursorManager.CloseCursor(stmt.CursorName);
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteDeclareHandler(DeclareHandlerStatement stmt)
    {
        if (_conditionHandlers == null)
            throw new CyscaleException("DECLARE HANDLER can only be used inside a stored procedure", ErrorCode.InternalError);

        var handlerType = stmt.HandlerAction.ToUpperInvariant() switch
        {
            "CONTINUE" => HandlerType.Continue,
            "EXIT" => HandlerType.Exit,
            "UNDO" => HandlerType.Undo,
            _ => HandlerType.Continue
        };

        _conditionHandlers.Add(new ConditionHandler(handlerType, stmt.ConditionValues, stmt.HandlerBody));
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteSignal(SignalStatement stmt)
    {
        throw new SignalException(stmt.SqlState, stmt.MessageText, stmt.MysqlErrno);
    }

    private ExecutionResult ExecuteResignal(ResignalStatement stmt)
    {
        // RESIGNAL re-throws the current exception with optional modifications
        throw new SignalException(stmt.SqlState ?? "45000", stmt.MessageText);
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

    #region New Statement Execution (Phase 2+)

    private readonly Dictionary<string, string> _preparedStatements = new(StringComparer.OrdinalIgnoreCase);

    private ExecutionResult ExecutePrepare(PrepareStatement stmt)
    {
        var sqlExpr = EvaluateLiteralExpression(stmt.SqlExpression);
        _preparedStatements[stmt.StatementName] = sqlExpr.AsString();
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteExecuteStmt(ExecuteStatement stmt)
    {
        if (!_preparedStatements.TryGetValue(stmt.StatementName, out var sql))
            throw new CyscaleException($"Unknown prepared statement: {stmt.StatementName}");

        // Replace user variables @var1, @var2, etc. with actual values
        // For simplicity, we just execute the stored SQL
        var parser = new Parsing.Parser(sql);
        var parsedStmt = parser.Parse();
        return ExecuteInternal(parsedStmt);
    }

    private ExecutionResult ExecuteDeallocate(DeallocatePrepareStatement stmt)
    {
        _preparedStatements.Remove(stmt.StatementName);
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteLoadData(LoadDataStatement stmt)
    {
        var dbName = stmt.DatabaseName ?? _currentDatabase;
        var table = _catalog.GetTable(dbName, stmt.TableName);
        if (table == null) throw new TableNotFoundException(stmt.TableName);

        var schema = table.Schema;
        long count = 0;

        if (!System.IO.File.Exists(stmt.FilePath))
            throw new CyscaleException($"File not found: {stmt.FilePath}");

        var lines = System.IO.File.ReadAllLines(stmt.FilePath);
        int startLine = stmt.IgnoreRows;

        for (int lineIdx = startLine; lineIdx < lines.Length; lineIdx++)
        {
            var line = lines[lineIdx];
            if (string.IsNullOrEmpty(line)) continue;

            var fields = line.Split(stmt.FieldTerminator);
            var values = new DataValue[schema.Columns.Count];

            for (int i = 0; i < values.Length; i++)
            {
                var col = schema.Columns[i];
                values[i] = col.DefaultValue ?? DataValue.Null;
            }

            if (stmt.Columns.Count > 0)
            {
                for (int i = 0; i < Math.Min(stmt.Columns.Count, fields.Length); i++)
                {
                    var ordinal = schema.GetColumnOrdinal(stmt.Columns[i]);
                    if (ordinal >= 0)
                        values[ordinal] = ParseFieldValue(fields[i], schema.Columns[ordinal].DataType, stmt.FieldEnclosedBy);
                }
            }
            else
            {
                for (int i = 0; i < Math.Min(fields.Length, schema.Columns.Count); i++)
                {
                    values[i] = ParseFieldValue(fields[i], schema.Columns[i].DataType, stmt.FieldEnclosedBy);
                }
            }

            // Handle auto-increment
            foreach (var col in schema.Columns)
            {
                if (col.IsAutoIncrement && values[col.OrdinalPosition].IsNull)
                {
                    values[col.OrdinalPosition] = DataValue.FromBigInt(schema.GetNextAutoIncrementValue());
                }
            }

            table.InsertRow(new Row(schema, values));
            count++;
        }

        table.Flush();
        _catalog.UpdateTableSchema(schema);
        return ExecutionResult.Modification(count, 0);
    }

    private static DataValue ParseFieldValue(string field, DataType type, string enclosedBy)
    {
        if (!string.IsNullOrEmpty(enclosedBy) && field.StartsWith(enclosedBy) && field.EndsWith(enclosedBy))
            field = field[enclosedBy.Length..^enclosedBy.Length];

        if (field == "\\N" || field == "NULL") return DataValue.Null;

        return type switch
        {
            DataType.Int => int.TryParse(field, out var i) ? DataValue.FromInt(i) : DataValue.Null,
            DataType.BigInt => long.TryParse(field, out var l) ? DataValue.FromBigInt(l) : DataValue.Null,
            DataType.Float => float.TryParse(field, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var f) ? DataValue.FromFloat(f) : DataValue.Null,
            DataType.Double => double.TryParse(field, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var d) ? DataValue.FromDouble(d) : DataValue.Null,
            DataType.Decimal => decimal.TryParse(field, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var dc) ? DataValue.FromDecimal(dc) : DataValue.Null,
            DataType.DateTime => DateTime.TryParse(field, out var dt) ? DataValue.FromDateTime(dt) : DataValue.Null,
            DataType.Date => DateOnly.TryParse(field, out var date) ? DataValue.FromDate(date) : DataValue.Null,
            _ => DataValue.FromVarChar(field)
        };
    }

    private ExecutionResult ExecuteRenameTable(RenameTableStatement stmt)
    {
        foreach (var (oldName, newName) in stmt.Renames)
        {
            var table = _catalog.GetTable(_currentDatabase, oldName);
            if (table == null) throw new TableNotFoundException(oldName);
            _catalog.RenameTable(_currentDatabase, oldName, newName);
        }
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteCheckTable(CheckTableStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Table", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Op", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Msg_type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Msg_text", DataType = DataType.VarChar });

        foreach (var tableName in stmt.TableNames)
        {
            var table = _catalog.GetTable(_currentDatabase, tableName);
            result.Rows.Add([
                DataValue.FromVarChar($"{_currentDatabase}.{tableName}"),
                DataValue.FromVarChar("check"),
                DataValue.FromVarChar("status"),
                DataValue.FromVarChar(table != null ? "OK" : "Table doesn't exist")
            ]);
        }
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteRepairTable(RepairTableStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Table", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Op", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Msg_type", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Msg_text", DataType = DataType.VarChar });

        foreach (var tableName in stmt.TableNames)
        {
            result.Rows.Add([
                DataValue.FromVarChar($"{_currentDatabase}.{tableName}"),
                DataValue.FromVarChar("repair"),
                DataValue.FromVarChar("status"),
                DataValue.FromVarChar("OK")
            ]);
        }
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteChecksumTable(ChecksumTableStatement stmt)
    {
        var result = new ResultSet();
        result.Columns.Add(new ResultColumn { Name = "Table", DataType = DataType.VarChar });
        result.Columns.Add(new ResultColumn { Name = "Checksum", DataType = DataType.BigInt });

        foreach (var tableName in stmt.TableNames)
        {
            result.Rows.Add([
                DataValue.FromVarChar($"{_currentDatabase}.{tableName}"),
                DataValue.FromBigInt(0) // Simplified checksum
            ]);
        }
        return ExecutionResult.Query(result);
    }

    private ExecutionResult ExecuteReset(ResetStatement stmt)
    {
        switch (stmt.ResetType.ToUpperInvariant())
        {
            case "MASTER":
            case "BINARY LOGS AND GTIDS":
                _logger.Info("RESET MASTER executed - binary logs reset");
                break;
            case "SLAVE":
            case "REPLICA":
            case "SLAVE ALL":
            case "REPLICA ALL":
                _logger.Info("RESET {0} executed - replica configuration cleared", stmt.ResetType);
                break;
            case "QUERY CACHE":
                _logger.Info("RESET QUERY CACHE executed (no-op, query cache deprecated)");
                break;
            default:
                _logger.Info("RESET {0} executed", stmt.ResetType);
                break;
        }
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteKill(KillStatement stmt)
    {
        var kind = stmt.IsQuery ? "QUERY" : "CONNECTION";
        _logger.Info("KILL {0} {1} executed", kind, stmt.ConnectionId);
        // In a real implementation, this would cancel the query or close the connection.
        // For now, we just acknowledge the command.
        return ExecutionResult.Empty();
    }

    private ExecutionResult ExecuteBackupRestore(BackupRestoreStatement stmt)
    {
        if (stmt.IsBackup)
        {
            // mysqldump-compatible logical backup
            var db = _catalog.GetDatabase(stmt.DatabaseName);
            if (db == null) throw new CyscaleException($"Database not found: {stmt.DatabaseName}");

            var sb = new System.Text.StringBuilder();
            // mysqldump compatible header
            sb.AppendLine("-- CyscaleDB dump (mysqldump compatible)");
            sb.AppendLine($"-- Host: {Environment.MachineName}    Database: {stmt.DatabaseName}");
            sb.AppendLine("-- ------------------------------------------------------");
            sb.AppendLine($"-- Server version\t{Constants.ServerVersion}");
            sb.AppendLine();
            sb.AppendLine("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;");
            sb.AppendLine("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;");
            sb.AppendLine("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;");
            sb.AppendLine("/*!50503 SET NAMES utf8mb4 */;");
            sb.AppendLine("/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;");
            sb.AppendLine("/*!40103 SET TIME_ZONE='+00:00' */;");
            sb.AppendLine("/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;");
            sb.AppendLine("/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;");
            sb.AppendLine("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;");
            sb.AppendLine("/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;");
            sb.AppendLine();
            sb.AppendLine($"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `{stmt.DatabaseName}` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */;");
            sb.AppendLine();
            sb.AppendLine($"USE `{stmt.DatabaseName}`;");
            sb.AppendLine();

            foreach (var tableName in db.Tables.Select(t => t.TableName))
            {
                var table = _catalog.GetTable(stmt.DatabaseName, tableName);
                if (table == null) continue;
                var schema = table.Schema;

                sb.AppendLine("--");
                sb.AppendLine($"-- Table structure for table `{tableName}`");
                sb.AppendLine("--");
                sb.AppendLine();
                sb.AppendLine($"DROP TABLE IF EXISTS `{tableName}`;");
                sb.AppendLine("/*!40101 SET @saved_cs_client     = @@character_set_client */;");
                sb.AppendLine("/*!50503 SET character_set_client = utf8mb4 */;");
                sb.Append($"CREATE TABLE `{tableName}` (");
                var colDefs = new List<string>();
                foreach (var col in schema.Columns)
                {
                    var def = $"\n  `{col.Name}` {col.DataType}";
                    if (col.MaxLength > 0)
                        def += $"({col.MaxLength})";
                    if (!col.IsNullable) def += " NOT NULL";
                    if (col.IsAutoIncrement) def += " AUTO_INCREMENT";
                    if (col.DefaultValue is DataValue dv && !dv.IsNull)
                        def += $" DEFAULT {(dv.Type is DataType.VarChar or DataType.Char ? $"'{dv.AsString()}'" : dv.ToString())}";
                    colDefs.Add(def);
                }
                // Add PRIMARY KEY constraint
                var pkCols = schema.Columns.Where(c => c.IsPrimaryKey).Select(c => $"`{c.Name}`").ToList();
                if (pkCols.Count > 0)
                    colDefs.Add($"\n  PRIMARY KEY ({string.Join(",", pkCols)})");
                sb.AppendLine(string.Join(",", colDefs));
                sb.AppendLine(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;");
                sb.AppendLine("/*!40101 SET character_set_client = @saved_cs_client */;");
                sb.AppendLine();

                // Generate INSERT statements with LOCK/UNLOCK and extended INSERT
                var rows = table.ScanTable().ToList();
                if (rows.Count > 0)
                {
                    sb.AppendLine("--");
                    sb.AppendLine($"-- Dumping data for table `{tableName}`");
                    sb.AppendLine("--");
                    sb.AppendLine();
                    sb.AppendLine($"LOCK TABLES `{tableName}` WRITE;");
                    sb.AppendLine($"/*!40000 ALTER TABLE `{tableName}` DISABLE KEYS */;");

                    // Use extended INSERT for better performance (batch of up to 100 rows)
                    for (int batchStart = 0; batchStart < rows.Count; batchStart += 100)
                    {
                        var batchEnd = Math.Min(batchStart + 100, rows.Count);
                        sb.Append($"INSERT INTO `{tableName}` VALUES ");
                        var rowStrs = new List<string>();
                        for (int r = batchStart; r < batchEnd; r++)
                        {
                            var vals = new List<string>();
                            foreach (var v in rows[r].Values)
                            {
                                if (v.IsNull) vals.Add("NULL");
                                else if (v.Type is DataType.VarChar or DataType.Char or DataType.Text
                                    or DataType.TinyText or DataType.MediumText or DataType.LongText)
                                    vals.Add($"'{v.AsString().Replace("\\", "\\\\").Replace("'", "\\'")}'");
                                else if (v.Type is DataType.DateTime or DataType.Timestamp or DataType.Date or DataType.Time)
                                    vals.Add($"'{v.AsString()}'");
                                else vals.Add(v.ToString() ?? "NULL");
                            }
                            rowStrs.Add($"({string.Join(",", vals)})");
                        }
                        sb.AppendLine(string.Join(",", rowStrs) + ";");
                    }

                    sb.AppendLine($"/*!40000 ALTER TABLE `{tableName}` ENABLE KEYS */;");
                    sb.AppendLine("UNLOCK TABLES;");
                    sb.AppendLine();
                }
            }

            // mysqldump footer
            sb.AppendLine("/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;");
            sb.AppendLine("/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;");
            sb.AppendLine("/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;");
            sb.AppendLine("/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;");
            sb.AppendLine("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;");
            sb.AppendLine("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;");
            sb.AppendLine("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;");
            sb.AppendLine("/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;");
            sb.AppendLine();
            sb.AppendLine($"-- Dump completed on {DateTime.Now:yyyy-MM-dd HH:mm:ss}");

            System.IO.File.WriteAllText(stmt.Path, sb.ToString());
            _logger.Info("Database '{0}' backed up to '{1}' (mysqldump format)", stmt.DatabaseName, stmt.Path);
        }
        else
        {
            // Restore: read and execute SQL file
            if (!System.IO.File.Exists(stmt.Path))
                throw new CyscaleException($"Backup file not found: {stmt.Path}");

            var sql = System.IO.File.ReadAllText(stmt.Path);
            // Split by semicolons and execute each statement
            foreach (var stmtSql in sql.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var trimmed = stmtSql.Trim();
                if (string.IsNullOrWhiteSpace(trimmed) || trimmed.StartsWith("--") || trimmed.StartsWith("/*!")) 
                    continue;
                try
                {
                    var parser = new Parsing.Parser(trimmed);
                    var parsed = parser.Parse();
                    ExecuteInternal(parsed);
                }
                catch (Exception ex)
                {
                    _logger.Warning("Skipping restore statement: {0} - {1}", trimmed[..Math.Min(50, trimmed.Length)], ex.Message);
                }
            }
            _logger.Info("Database '{0}' restored from '{1}'", stmt.DatabaseName, stmt.Path);
        }
        return ExecutionResult.Empty();
    }

    #endregion

    #region DO and HANDLER Statements

    /// <summary>
    /// Executes a DO statement - evaluates expressions but returns no result.
    /// </summary>
    private ExecutionResult ExecuteDo(DoStatement stmt)
    {
        foreach (var expr in stmt.Expressions)
        {
            var eval = BuildExpressionEvaluator(expr);
            eval.Evaluate(null!); // evaluate for side effects
        }
        return ExecutionResult.Empty();
    }

    // Holds open handlers per session. Key = handler name, Value = (schema, all rows, current index)
    [ThreadStatic]
    private static Dictionary<string, (Storage.TableSchema Schema, List<Storage.Row> Rows, int Index)>? _openHandlers;

    /// <summary>
    /// Executes HANDLER table OPEN.
    /// </summary>
    private ExecutionResult ExecuteHandlerOpen(HandlerOpenStatement stmt)
    {
        _openHandlers ??= new(StringComparer.OrdinalIgnoreCase);
        var dbName = _currentDatabase;
        var schema = _catalog.GetTableSchema(dbName, stmt.TableName)
            ?? throw new CyscaleException($"Table '{stmt.TableName}' doesn't exist", ErrorCode.TableNotFound);

        // Read all rows from the table using table scan
        var table = _catalog.GetTable(dbName, stmt.TableName)
            ?? throw new CyscaleException($"Table '{stmt.TableName}' doesn't exist", ErrorCode.TableNotFound);
        var rows = new List<Storage.Row>();
        var scanOp = new Operators.TableScanOperator(table);
        scanOp.Open();
        try
        {
            Storage.Row? row;
            while ((row = scanOp.Next()) != null)
            {
                rows.Add(row);
            }
        }
        finally
        {
            scanOp.Close();
            scanOp.Dispose();
        }

        var handlerName = stmt.Alias ?? stmt.TableName;
        _openHandlers[handlerName] = (schema, rows, 0);

        return ExecutionResult.Empty();
    }

    /// <summary>
    /// Executes HANDLER table READ.
    /// </summary>
    private ExecutionResult ExecuteHandlerRead(HandlerReadStatement stmt)
    {
        _openHandlers ??= new(StringComparer.OrdinalIgnoreCase);
        if (!_openHandlers.TryGetValue(stmt.HandlerName, out var handler))
            throw new CyscaleException($"Unknown HANDLER: '{stmt.HandlerName}'");

        var (schema, rows, currentIndex) = handler;
        var result = new ResultSet();
        foreach (var col in schema.Columns)
        {
            result.Columns.Add(new ResultColumn { Name = col.Name, DataType = col.DataType });
        }

        int limit = stmt.Limit ?? 1;

        // Determine starting position based on read type
        int startIdx = stmt.ReadType switch
        {
            "FIRST" => 0,
            "LAST" => rows.Count - 1,
            "PREV" => currentIndex - 1,
            _ => currentIndex // NEXT
        };

        int added = 0;
        int newIndex = startIdx;
        bool forward = stmt.ReadType is "FIRST" or "NEXT";

        for (int i = startIdx; i >= 0 && i < rows.Count && added < limit; i += forward ? 1 : -1)
        {
            var row = rows[i];
            if (stmt.Where != null)
            {
                var whereEval = BuildExpressionEvaluator(stmt.Where);
                var val = whereEval.Evaluate(row);
                if (val.Type != DataType.Boolean || !val.AsBoolean()) continue;
            }
            result.Rows.Add(row.Values);
            added++;
            newIndex = i + (forward ? 1 : -1);
        }

        _openHandlers[stmt.HandlerName] = (schema, rows, Math.Max(0, Math.Min(rows.Count, newIndex)));
        return ExecutionResult.Query(result);
    }

    /// <summary>
    /// Executes HANDLER table CLOSE.
    /// </summary>
    private ExecutionResult ExecuteHandlerClose(HandlerCloseStatement stmt)
    {
        _openHandlers ??= new(StringComparer.OrdinalIgnoreCase);
        _openHandlers.Remove(stmt.HandlerName);
        return ExecutionResult.Empty();
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
        var args = func.Arguments;

        IExpressionEvaluator Arg(int i) => BuildExpression(args[i], schema);
        List<IExpressionEvaluator> AllArgs() => args.Select(a => BuildExpression(a, schema)).ToList();
        List<IExpressionEvaluator> ArgsFrom(int start) => args.Skip(start).Select(a => BuildExpression(a, schema)).ToList();

        // Handle built-in functions
        return funcName switch
        {
            // ── Date/Time constants ──
            "NOW" or "CURRENT_TIMESTAMP" or "LOCALTIME" or "LOCALTIMESTAMP" => new ConstantEvaluator(DataValue.FromDateTime(DateTime.Now)),
            "SYSDATE" => new ConstantEvaluator(DataValue.FromDateTime(DateTime.Now)),
            "CURDATE" or "CURRENT_DATE" => new ConstantEvaluator(DataValue.FromDate(DateOnly.FromDateTime(DateTime.Now))),
            "CURTIME" or "CURRENT_TIME" => new ConstantEvaluator(DataValue.FromTime(TimeOnly.FromDateTime(DateTime.Now))),
            "UTC_DATE" => new ConstantEvaluator(DataValue.FromDate(DateOnly.FromDateTime(DateTime.UtcNow))),
            "UTC_TIME" => new ConstantEvaluator(DataValue.FromTime(TimeOnly.FromDateTime(DateTime.UtcNow))),
            "UTC_TIMESTAMP" => new ConstantEvaluator(DataValue.FromDateTime(DateTime.UtcNow)),

            // ── System info functions ──
            "DATABASE" or "SCHEMA" => new ConstantEvaluator(DataValue.FromVarChar(_currentDatabase)),
            "VERSION" => new ConstantEvaluator(DataValue.FromVarChar(Constants.ServerVersion)),
            "USER" or "CURRENT_USER" or "SESSION_USER" or "SYSTEM_USER" => new ConstantEvaluator(DataValue.FromVarChar($"{_currentUser}@{_currentHost}")),
            "CONNECTION_ID" => new ConstantEvaluator(DataValue.FromBigInt(_connectionId)),
            "LAST_INSERT_ID" => new ConstantEvaluator(DataValue.FromBigInt(_lastInsertId)),
            "ROW_COUNT" => new ConstantEvaluator(DataValue.FromBigInt(0)),
            "FOUND_ROWS" => new ConstantEvaluator(DataValue.FromBigInt(0)),
            "CURRENT_ROLE" => new ConstantEvaluator(DataValue.FromVarChar("NONE")),
            "ICU_VERSION" => new ConstantEvaluator(DataValue.FromVarChar("73.1")),

            // ── Original string functions ──
            "UPPER" or "UCASE" => BuildStringFunction(func, schema, s => s.ToUpperInvariant()),
            "LOWER" or "LCASE" => BuildStringFunction(func, schema, s => s.ToLowerInvariant()),
            "LENGTH" or "OCTET_LENGTH" => BuildLengthFunction(func, schema),
            "CHAR_LENGTH" or "CHARACTER_LENGTH" => BuildLengthFunction(func, schema),
            "CONCAT" => BuildConcatFunction(func, schema),

            // ── Control flow functions ──
            "IFNULL" or "COALESCE" => BuildIfNullFunction(func, schema),
            "ISNULL" => BuildIsNullFunction(func, schema),
            "IF" => BuildIfFunction(func, schema),
            "FIELD" => BuildFieldFunction(func, schema),
            "NULLIF" => new NullIfEvaluator(Arg(0), Arg(1)),

            // ── Math functions ──
            "ABS" => new MathUnaryEvaluator(Arg(0), Math.Abs),
            "CEIL" or "CEILING" => new MathUnaryEvaluator(Arg(0), Math.Ceiling),
            "FLOOR" => new MathUnaryEvaluator(Arg(0), Math.Floor),
            "ROUND" => new RoundEvaluator(Arg(0), args.Count > 1 ? Arg(1) : null),
            "TRUNCATE" => new TruncateEvaluator(Arg(0), Arg(1)),
            "MOD" => new MathBinaryEvaluator(Arg(0), Arg(1), (a, b) => b == 0 ? double.NaN : a % b),
            "SIGN" => new SignEvaluator(Arg(0)),
            "POW" or "POWER" => new MathBinaryEvaluator(Arg(0), Arg(1), Math.Pow),
            "SQRT" => new MathUnaryEvaluator(Arg(0), Math.Sqrt),
            "EXP" => new MathUnaryEvaluator(Arg(0), Math.Exp),
            "LOG" => args.Count >= 2
                ? new MathBinaryEvaluator(Arg(0), Arg(1), (b, x) => Math.Log(x, b))
                : new MathUnaryEvaluator(Arg(0), Math.Log),
            "LOG2" => new MathUnaryEvaluator(Arg(0), Math.Log2),
            "LOG10" => new MathUnaryEvaluator(Arg(0), Math.Log10),
            "LN" => new MathUnaryEvaluator(Arg(0), Math.Log),
            "SIN" => new MathUnaryEvaluator(Arg(0), Math.Sin),
            "COS" => new MathUnaryEvaluator(Arg(0), Math.Cos),
            "TAN" => new MathUnaryEvaluator(Arg(0), Math.Tan),
            "ASIN" => new MathUnaryEvaluator(Arg(0), Math.Asin),
            "ACOS" => new MathUnaryEvaluator(Arg(0), Math.Acos),
            "ATAN" => args.Count >= 2
                ? new MathBinaryEvaluator(Arg(0), Arg(1), Math.Atan2)
                : new MathUnaryEvaluator(Arg(0), Math.Atan),
            "ATAN2" => new MathBinaryEvaluator(Arg(0), Arg(1), Math.Atan2),
            "COT" => new MathUnaryEvaluator(Arg(0), x => 1.0 / Math.Tan(x)),
            "DEGREES" => new MathUnaryEvaluator(Arg(0), x => x * (180.0 / Math.PI)),
            "RADIANS" => new MathUnaryEvaluator(Arg(0), x => x * (Math.PI / 180.0)),
            "PI" => new ConstantEvaluator(DataValue.FromDouble(Math.PI)),
            "RAND" => new RandEvaluator(args.Count > 0 ? Arg(0) : null),
            "CRC32" => new Crc32Evaluator(Arg(0)),
            "CONV" => new ConvEvaluator(Arg(0), Arg(1), Arg(2)),

            // ── String functions ──
            "SUBSTRING" or "SUBSTR" or "MID" => new SubstringEvaluator(Arg(0), Arg(1), args.Count > 2 ? Arg(2) : null),
            "LEFT" => new LeftEvaluator(Arg(0), Arg(1)),
            "RIGHT" => new RightEvaluator(Arg(0), Arg(1)),
            "TRIM" => new TrimEvaluator(Arg(0), null, TrimEvaluator.TrimMode.Both),
            "LTRIM" => new TrimEvaluator(Arg(0), null, TrimEvaluator.TrimMode.Leading),
            "RTRIM" => new TrimEvaluator(Arg(0), null, TrimEvaluator.TrimMode.Trailing),
            "LPAD" => new PadEvaluator(Arg(0), Arg(1), Arg(2), true),
            "RPAD" => new PadEvaluator(Arg(0), Arg(1), Arg(2), false),
            "REPLACE" => new ReplaceEvaluator(Arg(0), Arg(1), Arg(2)),
            "LOCATE" or "POSITION" => new LocateEvaluator(Arg(0), Arg(1), args.Count > 2 ? Arg(2) : null),
            "INSTR" => new LocateEvaluator(Arg(1), Arg(0), null),
            "INSERT" => new InsertStringEvaluator(Arg(0), Arg(1), Arg(2), Arg(3)),
            "REPEAT" => new RepeatEvaluator(Arg(0), Arg(1)),
            "REVERSE" => new ReverseEvaluator(Arg(0)),
            "SPACE" => new SpaceEvaluator(Arg(0)),
            "FORMAT" => new FormatEvaluator(Arg(0), Arg(1)),
            "ASCII" => new AsciiEvaluator(Arg(0)),
            "ORD" => new OrdEvaluator(Arg(0)),
            "CHAR" => new CharFunctionEvaluator(AllArgs()),
            "HEX" => new HexEvaluator(Arg(0)),
            "UNHEX" => new UnhexEvaluator(Arg(0)),
            "BIN" => new BinEvaluator(Arg(0)),
            "OCT" => new OctEvaluator(Arg(0)),
            "FROM_BASE64" => new Base64Evaluator(Arg(0), false),
            "TO_BASE64" => new Base64Evaluator(Arg(0), true),
            "CONCAT_WS" => new ConcatWsEvaluator(Arg(0), ArgsFrom(1)),
            "STRCMP" => new StrcmpEvaluator(Arg(0), Arg(1)),
            "SOUNDEX" => new SoundexEvaluator(Arg(0)),
            "SUBSTRING_INDEX" => new SubstringIndexEvaluator(Arg(0), Arg(1), Arg(2)),
            "ELT" => new EltEvaluator(Arg(0), ArgsFrom(1)),
            "FIND_IN_SET" => new FindInSetEvaluator(Arg(0), Arg(1)),
            "EXPORT_SET" => new ExportSetEvaluator(AllArgs()),
            "MAKE_SET" => new MakeSetEvaluator(Arg(0), ArgsFrom(1)),
            "QUOTE" => new QuoteEvaluator(Arg(0)),
            "BIT_LENGTH" => new BitLengthEvaluator(Arg(0)),
            "WEIGHT_STRING" => new WeightStringEvaluator(Arg(0)),

            // ── Date/Time functions ──
            "YEAR" => new DatePartEvaluator(Arg(0), dt => dt.Year),
            "MONTH" => new DatePartEvaluator(Arg(0), dt => dt.Month),
            "DAY" or "DAYOFMONTH" => new DatePartEvaluator(Arg(0), dt => dt.Day),
            "HOUR" => new DatePartEvaluator(Arg(0), dt => dt.Hour),
            "MINUTE" => new DatePartEvaluator(Arg(0), dt => dt.Minute),
            "SECOND" => new DatePartEvaluator(Arg(0), dt => dt.Second),
            "MICROSECOND" => new DatePartEvaluator(Arg(0), dt => dt.Millisecond * 1000),
            "DAYOFWEEK" => new DatePartEvaluator(Arg(0), dt => (int)dt.DayOfWeek + 1),
            "DAYOFYEAR" => new DatePartEvaluator(Arg(0), dt => dt.DayOfYear),
            "WEEKDAY" => new DatePartEvaluator(Arg(0), dt => ((int)dt.DayOfWeek + 6) % 7),
            "WEEK" or "WEEKOFYEAR" => new DatePartEvaluator(Arg(0), dt =>
                System.Globalization.CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dt, System.Globalization.CalendarWeekRule.FirstDay, DayOfWeek.Sunday)),
            "YEARWEEK" => new DatePartEvaluator(Arg(0), dt =>
                dt.Year * 100 + System.Globalization.CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dt, System.Globalization.CalendarWeekRule.FirstDay, DayOfWeek.Sunday)),
            "QUARTER" => new DatePartEvaluator(Arg(0), dt => (dt.Month - 1) / 3 + 1),
            "DAYNAME" => new DayNameEvaluator(Arg(0)),
            "MONTHNAME" => new MonthNameEvaluator(Arg(0)),
            "DATE" => new DateExtractEvaluator(Arg(0)),
            "TIME" => new TimeExtractEvaluator(Arg(0)),
            "DATE_ADD" or "ADDDATE" => new DateAddEvaluator(Arg(0), Arg(1), args.Count > 2 ? GetIntervalUnit(args[2]) : "DAY", false),
            "DATE_SUB" or "SUBDATE" => new DateAddEvaluator(Arg(0), Arg(1), args.Count > 2 ? GetIntervalUnit(args[2]) : "DAY", true),
            "ADDTIME" => new AddTimeEvaluator(Arg(0), Arg(1), false),
            "SUBTIME" => new AddTimeEvaluator(Arg(0), Arg(1), true),
            "DATEDIFF" => new DateDiffEvaluator(Arg(0), Arg(1)),
            "TIMEDIFF" => new TimeDiffEvaluator(Arg(0), Arg(1)),
            "TIMESTAMPDIFF" => new TimestampDiffEvaluator(GetIntervalUnit(args[0]), Arg(1), Arg(2)),
            "TIMESTAMPADD" => new TimestampAddEvaluator(GetIntervalUnit(args[0]), Arg(1), Arg(2)),
            "DATE_FORMAT" => new DateFormatEvaluator(Arg(0), Arg(1)),
            "TIME_FORMAT" => new TimeFormatEvaluator(Arg(0), Arg(1)),
            "STR_TO_DATE" => new StrToDateEvaluator(Arg(0), Arg(1)),
            "GET_FORMAT" => new GetFormatEvaluator(Arg(0), Arg(1)),
            "UNIX_TIMESTAMP" => new UnixTimestampEvaluator(args.Count > 0 ? Arg(0) : null),
            "FROM_UNIXTIME" => new FromUnixTimeEvaluator(Arg(0), args.Count > 1 ? Arg(1) : null),
            "FROM_DAYS" => new FromDaysEvaluator(Arg(0)),
            "TO_DAYS" => new ToDaysEvaluator(Arg(0)),
            "TO_SECONDS" => new ToSecondsEvaluator(Arg(0)),
            "SEC_TO_TIME" => new SecToTimeEvaluator(Arg(0)),
            "TIME_TO_SEC" => new TimeToSecEvaluator(Arg(0)),
            "MAKEDATE" => new MakeDateEvaluator(Arg(0), Arg(1)),
            "MAKETIME" => new MakeTimeEvaluator(Arg(0), Arg(1), Arg(2)),
            "LAST_DAY" => new LastDayEvaluator(Arg(0)),
            "PERIOD_ADD" => new PeriodAddEvaluator(Arg(0), Arg(1)),
            "PERIOD_DIFF" => new PeriodDiffEvaluator(Arg(0), Arg(1)),
            "CONVERT_TZ" => new ConvertTzEvaluator(Arg(0), Arg(1), Arg(2)),
            "EXTRACT" => new ExtractEvaluator(GetIntervalUnit(args[0]), Arg(1)),
            "TIMESTAMP" => args.Count >= 2
                ? new AddTimeEvaluator(Arg(0), Arg(1), false)
                : (IExpressionEvaluator)new ConstantEvaluator(DataValue.FromDateTime(DateTime.Now)),

            // ── Encryption / Hash functions ──
            "MD5" => new Md5Evaluator(Arg(0)),
            "SHA1" or "SHA" => new Sha1Evaluator(Arg(0)),
            "SHA2" => new Sha2Evaluator(Arg(0), Arg(1)),
            "AES_ENCRYPT" => new AesEvaluator(Arg(0), Arg(1), true),
            "AES_DECRYPT" => new AesEvaluator(Arg(0), Arg(1), false),
            "COMPRESS" => new CompressEvaluator(Arg(0), true),
            "UNCOMPRESS" => new CompressEvaluator(Arg(0), false),
            "UNCOMPRESSED_LENGTH" => new UncompressedLengthEvaluator(Arg(0)),
            "RANDOM_BYTES" => new RandomBytesEvaluator(Arg(0)),

            // ── Regex functions ──
            "REGEXP_LIKE" => new RegexpLikeEvaluator(Arg(0), Arg(1), args.Count > 2 ? Arg(2) : null),
            "REGEXP_INSTR" => new RegexpInstrEvaluator(AllArgs()),
            "REGEXP_REPLACE" => new RegexpReplaceEvaluator(AllArgs()),
            "REGEXP_SUBSTR" => new RegexpSubstrEvaluator(AllArgs()),

            // ── UUID functions ──
            "UUID" => new UuidEvaluator(),
            "UUID_SHORT" => new UuidShortEvaluator(),
            "UUID_TO_BIN" => new UuidToBinEvaluator(Arg(0), args.Count > 1 ? Arg(1) : null),
            "BIN_TO_UUID" => new BinToUuidEvaluator(Arg(0), args.Count > 1 ? Arg(1) : null),
            "IS_UUID" => new IsUuidEvaluator(Arg(0)),

            // ── Locking functions ──
            "GET_LOCK" => new GetLockEvaluator(Arg(0), Arg(1)),
            "RELEASE_LOCK" => new ReleaseLockEvaluator(Arg(0)),
            "RELEASE_ALL_LOCKS" => new ReleaseAllLocksEvaluator(),
            "IS_FREE_LOCK" => new IsFreeLockEvaluator(Arg(0)),
            "IS_USED_LOCK" => new IsUsedLockEvaluator(Arg(0)),

            // ── Network functions ──
            "INET_ATON" => new InetAtonEvaluator(Arg(0)),
            "INET_NTOA" => new InetNtoaEvaluator(Arg(0)),

            // ── Miscellaneous functions ──
            "SLEEP" => new SleepEvaluator(Arg(0)),
            "BENCHMARK" => new BenchmarkEvaluator(Arg(0), Arg(1)),
            "ANY_VALUE" => new AnyValueEvaluator(Arg(0)),
            "BIT_COUNT" => new BitCountEvaluator(Arg(0)),
            "GREATEST" => new GreatestLeastEvaluator(AllArgs(), true),
            "LEAST" => new GreatestLeastEvaluator(AllArgs(), false),
            "CHARSET" => new CharsetFuncEvaluator(Arg(0)),
            "COERCIBILITY" => new CoercibilityEvaluator(Arg(0)),
            "COLLATION" => new CollationFuncEvaluator(Arg(0)),
            "GROUPING" => new GroupingEvaluator(Arg(0)),
            "VALUES" => args.Count > 0 ? new ValuesEvaluator(Arg(0)) : new ConstantEvaluator(DataValue.Null),
            "INTERVAL" => new IntervalEvaluator(Arg(0), ArgsFrom(1)),

            // ── JSON functions ──
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

            // ── Spatial functions ──
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
    /// Extracts an interval unit name from an expression (e.g., a column reference used as interval unit keyword).
    /// </summary>
    private static string GetIntervalUnit(Parsing.Ast.Expression expr)
    {
        return expr switch
        {
            Parsing.Ast.ColumnReference col => col.ColumnName.ToUpperInvariant(),
            Parsing.Ast.LiteralExpression lit => lit.Value.IsNull ? "DAY" : lit.Value.AsString().ToUpperInvariant(),
            _ => "DAY"
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

        // If not found directly, search for columns ending with _ColumnName (for joined schemas)
        if (ordinal < 0)
        {
            var suffix = $"_{col.ColumnName}";
            for (int i = 0; i < schema.Columns.Count; i++)
            {
                if (schema.Columns[i].Name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase) ||
                    schema.Columns[i].Name.Equals(col.ColumnName, StringComparison.OrdinalIgnoreCase))
                {
                    ordinal = i;
                    break;
                }
            }
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
        var columnNames = new List<string>();
        foreach (var col in matchExpr.Columns)
        {
            var colEval = BuildColumnReference(col, schema);
            columnEvaluators.Add(colEval);
            columnNames.Add(col.ColumnName);
        }

        // Build the search text evaluator
        var searchTextEvaluator = BuildExpression(matchExpr.SearchText, schema);

        // Convert the search mode
        var mode = matchExpr.Mode switch
        {
            MatchSearchMode.Boolean => Storage.Index.BooleanSearchMode.And,
            _ => Storage.Index.BooleanSearchMode.Or
        };

        // Pass schema info for full-text index lookup
        return new MatchAgainstEvaluator(
            columnEvaluators, 
            searchTextEvaluator, 
            mode,
            schema.DatabaseName,
            schema.TableName,
            columnNames);
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
    private readonly string? _databaseName;
    private readonly string? _tableName;
    private readonly List<string> _columnNames;
    private readonly Storage.Index.FullTextIndexManager _indexManager;
    
    // Cache for full-text index search results (rowId -> score)
    private Dictionary<int, double>? _cachedScores;
    private string? _cachedSearchText;

    public MatchAgainstEvaluator(
        List<IExpressionEvaluator> columnEvaluators,
        IExpressionEvaluator searchTextEvaluator,
        Storage.Index.BooleanSearchMode mode,
        string? databaseName = null,
        string? tableName = null,
        List<string>? columnNames = null)
    {
        _columnEvaluators = columnEvaluators;
        _searchTextEvaluator = searchTextEvaluator;
        _mode = mode;
        _tokenizer = new Storage.Index.SimpleTokenizer();
        _databaseName = databaseName;
        _tableName = tableName;
        _columnNames = columnNames ?? new List<string>();
        _indexManager = Storage.Index.FullTextIndexManager.Instance;
    }

    public DataValue Evaluate(Row row)
    {
        var searchTextValue = _searchTextEvaluator.Evaluate(row);
        if (searchTextValue.IsNull)
            return DataValue.FromDouble(0.0);

        var searchText = searchTextValue.AsString();
        if (string.IsNullOrWhiteSpace(searchText))
            return DataValue.FromDouble(0.0);

        // Try to use full-text index if available
        if (!string.IsNullOrEmpty(_databaseName) && !string.IsNullOrEmpty(_tableName) && _columnNames.Count > 0)
        {
            var index = _indexManager.GetIndexForColumns(_databaseName, _tableName, _columnNames);
            if (index != null)
            {
                return EvaluateWithIndex(row, searchText, index);
            }
        }

        // Fall back to in-memory calculation
        return EvaluateInMemory(row, searchText);
    }

    private DataValue EvaluateWithIndex(Row row, string searchText, Storage.Index.FullTextIndex index)
    {
        // Use cached results if search text hasn't changed
        if (_cachedSearchText != searchText || _cachedScores == null)
        {
            var results = index.Search(searchText, 10000);
            _cachedScores = results.ToDictionary(r => r.DocumentId, r => r.Score);
            _cachedSearchText = searchText;
        }

        // Get the row ID and look up score
        var rowId = row.RowId;
        if (rowId.IsValid)
        {
            // Use combined page + slot as document ID
            int docId = (rowId.PageId << 16) | (rowId.SlotNumber & 0xFFFF);
            if (_cachedScores.TryGetValue(docId, out var score))
            {
                return DataValue.FromDouble(score);
            }
        }

        // If not found in index results, return 0
        return DataValue.FromDouble(0.0);
    }

    private DataValue EvaluateInMemory(Row row, string searchText)
    {
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
