using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;

namespace CyscaleDB.Core.Storage.PerformanceSchema;

/// <summary>
/// Provides virtual tables for the performance_schema database.
/// Implements MySQL 8.4 compatible performance instrumentation tables.
/// </summary>
public sealed class PerformanceSchemaProvider
{
    private readonly Catalog _catalog;

    /// <summary>
    /// Database name constant.
    /// </summary>
    public const string DatabaseName = "performance_schema";

    /// <summary>
    /// Supported virtual tables in performance_schema.
    /// </summary>
    public static readonly string[] SupportedTables =
    [
        // Setup tables
        "setup_actors",
        "setup_consumers",
        "setup_instruments",
        "setup_objects",
        "setup_threads",

        // Instance tables
        "mutex_instances",
        "rwlock_instances",
        "cond_instances",
        "file_instances",
        "socket_instances",

        // Wait event tables
        "events_waits_current",
        "events_waits_history",
        "events_waits_history_long",
        "events_waits_summary_by_instance",
        "events_waits_summary_by_thread_by_event_name",
        "events_waits_summary_global_by_event_name",

        // Stage event tables
        "events_stages_current",
        "events_stages_history",
        "events_stages_history_long",
        "events_stages_summary_by_thread_by_event_name",
        "events_stages_summary_global_by_event_name",

        // Statement event tables
        "events_statements_current",
        "events_statements_history",
        "events_statements_history_long",
        "events_statements_summary_by_digest",
        "events_statements_summary_by_thread_by_event_name",
        "events_statements_summary_global_by_event_name",

        // Transaction event tables
        "events_transactions_current",
        "events_transactions_history",
        "events_transactions_history_long",
        "events_transactions_summary_by_thread_by_event_name",
        "events_transactions_summary_global_by_event_name",

        // Connection tables
        "threads",
        "session_connect_attrs",
        "session_account_connect_attrs",
        "accounts",
        "hosts",
        "users",

        // Memory tables
        "memory_summary_by_thread_by_event_name",
        "memory_summary_global_by_event_name",
        "memory_summary_by_account_by_event_name",

        // Lock tables
        "metadata_locks",
        "table_handles",
        "data_locks",
        "data_lock_waits",

        // Replication tables
        "replication_connection_configuration",
        "replication_connection_status",
        "replication_applier_configuration",
        "replication_applier_status",
        "replication_applier_status_by_coordinator",
        "replication_applier_status_by_worker",

        // Variables tables
        "global_variables",
        "session_variables",
        "variables_by_thread",
        "global_status",
        "session_status",
        "status_by_thread",
        "persisted_variables",

        // Error/log tables
        "error_log",

        // Summary tables
        "file_summary_by_event_name",
        "file_summary_by_instance",
        "table_io_waits_summary_by_table",
        "table_io_waits_summary_by_index_usage",
        "table_lock_waits_summary_by_table",
        "socket_summary_by_instance",
        "socket_summary_by_event_name",
        "prepared_statements_instances",
    ];

    public PerformanceSchemaProvider(Catalog catalog)
    {
        _catalog = catalog;
    }

    /// <summary>
    /// Checks if a table name belongs to performance_schema.
    /// </summary>
    public static bool IsPerformanceSchemaTable(string tableName)
    {
        return SupportedTables.Any(t => t.Equals(tableName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Queries a performance_schema virtual table and returns a ResultSet.
    /// </summary>
    public ResultSet Query(string tableName)
    {
        return tableName.ToLowerInvariant() switch
        {
            "threads" => QueryThreads(),
            "global_variables" => QueryGlobalVariables(),
            "session_variables" => QuerySessionVariables(),
            "global_status" => QueryGlobalStatus(),
            "session_status" => QuerySessionStatus(),
            "setup_instruments" => QuerySetupInstruments(),
            "setup_consumers" => QuerySetupConsumers(),
            "setup_actors" => QuerySetupActors(),
            "events_statements_current" => QueryEventsStatementsCurrent(),
            "events_statements_history" => QueryEventsStatementsHistory(),
            "events_statements_summary_by_digest" => QueryStatementsSummaryByDigest(),
            "metadata_locks" => QueryMetadataLocks(),
            "data_locks" => QueryDataLocks(),
            "data_lock_waits" => QueryDataLockWaits(),
            "accounts" => QueryAccounts(),
            "users" => QueryUsers(),
            "hosts" => QueryHosts(),
            "error_log" => QueryErrorLog(),
            "prepared_statements_instances" => QueryPreparedStatements(),
            "replication_connection_status" => QueryReplicationConnectionStatus(),
            "replication_applier_status" => QueryReplicationApplierStatus(),
            "persisted_variables" => QueryPersistedVariables(),
            _ => QueryEmpty(tableName) // Return empty result for unimplemented tables
        };
    }

    /// <summary>
    /// Gets the column definitions for a performance_schema table.
    /// </summary>
    public ResultSet GetColumns(string tableName)
    {
        // For unrecognized tables, return the basic structure from Query
        return Query(tableName);
    }

    private ResultSet QueryThreads()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TYPE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PROCESSLIST_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "PROCESSLIST_USER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PROCESSLIST_HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PROCESSLIST_DB", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PROCESSLIST_COMMAND", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PROCESSLIST_TIME", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "PROCESSLIST_STATE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PROCESSLIST_INFO", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "INSTRUMENTED", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "HISTORY", DataType = DataType.VarChar });

        // Background threads
        rs.Rows.Add([
            DataValue.FromBigInt(1), DataValue.FromVarChar("thread/sql/main"),
            DataValue.FromVarChar("BACKGROUND"), DataValue.Null,
            DataValue.Null, DataValue.Null, DataValue.Null,
            DataValue.Null, DataValue.Null, DataValue.Null, DataValue.Null,
            DataValue.FromVarChar("YES"), DataValue.FromVarChar("YES")
        ]);
        // Current user thread
        rs.Rows.Add([
            DataValue.FromBigInt(2), DataValue.FromVarChar("thread/sql/one_connection"),
            DataValue.FromVarChar("FOREGROUND"), DataValue.FromBigInt(1),
            DataValue.FromVarChar("root"), DataValue.FromVarChar("localhost"),
            DataValue.Null, DataValue.FromVarChar("Query"),
            DataValue.FromBigInt(0), DataValue.FromVarChar("executing"),
            DataValue.FromVarChar("SELECT * FROM performance_schema.threads"),
            DataValue.FromVarChar("YES"), DataValue.FromVarChar("YES")
        ]);
        return rs;
    }

    private ResultSet QueryGlobalVariables()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "VARIABLE_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "VARIABLE_VALUE", DataType = DataType.VarChar });
        foreach (var kv in SystemVariables.GetAllGlobal())
        {
            rs.Rows.Add([
                DataValue.FromVarChar(kv.Key),
                SystemVariables.ToDataValue(kv.Value)
            ]);
        }
        return rs;
    }

    private ResultSet QuerySessionVariables()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "VARIABLE_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "VARIABLE_VALUE", DataType = DataType.VarChar });
        // Session variables are per-connection; return global as fallback
        foreach (var kv in SystemVariables.GetAllGlobal())
        {
            rs.Rows.Add([
                DataValue.FromVarChar(kv.Key),
                SystemVariables.ToDataValue(kv.Value)
            ]);
        }
        return rs;
    }

    private ResultSet QueryGlobalStatus()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "VARIABLE_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "VARIABLE_VALUE", DataType = DataType.VarChar });
        var uptime = (long)(DateTime.UtcNow - System.Diagnostics.Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalSeconds;
        var statusVars = new (string name, string value)[]
        {
            ("Uptime", uptime.ToString()),
            ("Threads_connected", "1"),
            ("Threads_running", "1"),
            ("Questions", "0"),
            ("Connections", "1"),
            ("Bytes_received", "0"),
            ("Bytes_sent", "0"),
            ("Innodb_buffer_pool_pages_total", "8192"),
            ("Innodb_buffer_pool_pages_free", "8191"),
        };
        foreach (var (name, value) in statusVars)
        {
            rs.Rows.Add([DataValue.FromVarChar(name), DataValue.FromVarChar(value)]);
        }
        return rs;
    }

    private ResultSet QuerySessionStatus() => QueryGlobalStatus(); // Same for single-session

    private ResultSet QuerySetupInstruments()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ENABLED", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TIMED", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PROPERTIES", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "FLAGS", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "VOLATILITY", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "DOCUMENTATION", DataType = DataType.VarChar });
        var instruments = new[]
        {
            "statement/sql/select", "statement/sql/insert", "statement/sql/update",
            "statement/sql/delete", "statement/sql/create_table", "statement/sql/drop_table",
            "wait/io/file/innodb/innodb_data_file", "wait/io/file/innodb/innodb_log_file",
            "wait/synch/mutex/innodb/buf_pool_mutex", "wait/synch/rwlock/innodb/btr_search_latch",
            "transaction", "memory/sql/THD::main_mem_root",
        };
        foreach (var name in instruments)
        {
            rs.Rows.Add([
                DataValue.FromVarChar(name), DataValue.FromVarChar("YES"),
                DataValue.FromVarChar("YES"), DataValue.Null, DataValue.Null,
                DataValue.FromInt(0), DataValue.Null
            ]);
        }
        return rs;
    }

    private ResultSet QuerySetupConsumers()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ENABLED", DataType = DataType.VarChar });
        var consumers = new[]
        {
            ("events_stages_current", "YES"), ("events_stages_history", "YES"),
            ("events_stages_history_long", "NO"),
            ("events_statements_current", "YES"), ("events_statements_history", "YES"),
            ("events_statements_history_long", "NO"),
            ("events_transactions_current", "YES"), ("events_transactions_history", "YES"),
            ("events_transactions_history_long", "NO"),
            ("events_waits_current", "YES"), ("events_waits_history", "YES"),
            ("events_waits_history_long", "NO"),
            ("global_instrumentation", "YES"), ("thread_instrumentation", "YES"),
            ("statements_digest", "YES"),
        };
        foreach (var (name, enabled) in consumers)
        {
            rs.Rows.Add([DataValue.FromVarChar(name), DataValue.FromVarChar(enabled)]);
        }
        return rs;
    }

    private ResultSet QuerySetupActors()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "USER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ROLE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ENABLED", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "HISTORY", DataType = DataType.VarChar });
        rs.Rows.Add([
            DataValue.FromVarChar("%"), DataValue.FromVarChar("%"),
            DataValue.FromVarChar("%"), DataValue.FromVarChar("YES"),
            DataValue.FromVarChar("YES")
        ]);
        return rs;
    }

    private ResultSet QueryEventsStatementsCurrent()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "EVENT_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "EVENT_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "SQL_TEXT", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DIGEST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DIGEST_TEXT", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TIMER_START", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "TIMER_END", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "TIMER_WAIT", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "ROWS_AFFECTED", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "ROWS_SENT", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "ROWS_EXAMINED", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "ERRORS", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "WARNINGS", DataType = DataType.BigInt });
        return rs;
    }

    private ResultSet QueryEventsStatementsHistory() => QueryEventsStatementsCurrent();

    private ResultSet QueryStatementsSummaryByDigest()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "SCHEMA_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DIGEST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DIGEST_TEXT", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "COUNT_STAR", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "SUM_TIMER_WAIT", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "MIN_TIMER_WAIT", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "AVG_TIMER_WAIT", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "MAX_TIMER_WAIT", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "SUM_ROWS_AFFECTED", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "SUM_ROWS_SENT", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "SUM_ROWS_EXAMINED", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "FIRST_SEEN", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "LAST_SEEN", DataType = DataType.DateTime });
        return rs;
    }

    private ResultSet QueryMetadataLocks()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "OBJECT_TYPE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "OBJECT_SCHEMA", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "OBJECT_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "COLUMN_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "OBJECT_INSTANCE_BEGIN", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "LOCK_TYPE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "LOCK_DURATION", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "LOCK_STATUS", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "SOURCE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "OWNER_THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "OWNER_EVENT_ID", DataType = DataType.BigInt });
        return rs;
    }

    private ResultSet QueryDataLocks()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "ENGINE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ENGINE_LOCK_ID", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ENGINE_TRANSACTION_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "EVENT_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "OBJECT_SCHEMA", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "OBJECT_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PARTITION_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "INDEX_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "LOCK_TYPE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "LOCK_MODE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "LOCK_STATUS", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "LOCK_DATA", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryDataLockWaits()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "ENGINE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "REQUESTING_ENGINE_LOCK_ID", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "REQUESTING_ENGINE_TRANSACTION_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "REQUESTING_THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "REQUESTING_EVENT_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "BLOCKING_ENGINE_LOCK_ID", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "BLOCKING_ENGINE_TRANSACTION_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "BLOCKING_THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "BLOCKING_EVENT_ID", DataType = DataType.BigInt });
        return rs;
    }

    private ResultSet QueryAccounts()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "USER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "CURRENT_CONNECTIONS", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "TOTAL_CONNECTIONS", DataType = DataType.BigInt });
        rs.Rows.Add([
            DataValue.FromVarChar("root"), DataValue.FromVarChar("localhost"),
            DataValue.FromBigInt(1), DataValue.FromBigInt(1)
        ]);
        return rs;
    }

    private ResultSet QueryUsers()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "USER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "CURRENT_CONNECTIONS", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "TOTAL_CONNECTIONS", DataType = DataType.BigInt });
        rs.Rows.Add([
            DataValue.FromVarChar("root"), DataValue.FromBigInt(1), DataValue.FromBigInt(1)
        ]);
        return rs;
    }

    private ResultSet QueryHosts()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "CURRENT_CONNECTIONS", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "TOTAL_CONNECTIONS", DataType = DataType.BigInt });
        rs.Rows.Add([
            DataValue.FromVarChar("localhost"), DataValue.FromBigInt(1), DataValue.FromBigInt(1)
        ]);
        return rs;
    }

    private ResultSet QueryErrorLog()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "LOGGED", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "PRIO", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ERROR_CODE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "SUBSYSTEM", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DATA", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryPreparedStatements()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "OBJECT_INSTANCE_BEGIN", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "STATEMENT_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "STATEMENT_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "SQL_TEXT", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "OWNER_THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "OWNER_EVENT_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "OWNER_OBJECT_TYPE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "OWNER_OBJECT_SCHEMA", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "OWNER_OBJECT_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TIMER_PREPARE", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "COUNT_REPREPARE", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "COUNT_EXECUTE", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "SUM_TIMER_EXECUTE", DataType = DataType.BigInt });
        return rs;
    }

    private ResultSet QueryReplicationConnectionStatus()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "CHANNEL_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "GROUP_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "SOURCE_UUID", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "THREAD_ID", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "SERVICE_STATE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "RECEIVED_TRANSACTION_SET", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "LAST_ERROR_NUMBER", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "LAST_ERROR_MESSAGE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "LAST_ERROR_TIMESTAMP", DataType = DataType.DateTime });
        return rs;
    }

    private ResultSet QueryReplicationApplierStatus()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "CHANNEL_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "SERVICE_STATE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "REMAINING_DELAY", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "COUNT_TRANSACTIONS_RETRIES", DataType = DataType.BigInt });
        return rs;
    }

    private ResultSet QueryPersistedVariables()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "VARIABLE_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "VARIABLE_VALUE", DataType = DataType.VarChar });
        return rs;
    }

    /// <summary>
    /// Returns an empty result set with generic columns for unimplemented tables.
    /// </summary>
    private ResultSet QueryEmpty(string tableName)
    {
        var rs = new ResultSet();
        // Return at least a placeholder column
        rs.Columns.Add(new ResultColumn { Name = "PLACEHOLDER", DataType = DataType.VarChar });
        return rs;
    }
}
