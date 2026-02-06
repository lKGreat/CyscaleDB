using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;

namespace CyscaleDB.Core.Storage.PerformanceSchema;

/// <summary>
/// Provides virtual tables/views for the sys schema database.
/// The sys schema provides helper views over performance_schema for easier diagnostics.
/// Compatible with MySQL 8.4 sys schema.
/// </summary>
public sealed class SysSchemaProvider
{
    private readonly PerformanceSchemaProvider _perfSchema;

    /// <summary>
    /// Database name constant.
    /// </summary>
    public const string DatabaseName = "sys";

    /// <summary>
    /// Supported views in the sys schema.
    /// </summary>
    public static readonly string[] SupportedViews =
    [
        // Host summary views
        "host_summary",
        "host_summary_by_file_io",
        "host_summary_by_file_io_type",
        "host_summary_by_stages",
        "host_summary_by_statement_latency",
        "host_summary_by_statement_type",

        // IO views
        "io_by_thread_by_latency",
        "io_global_by_file_by_bytes",
        "io_global_by_file_by_latency",
        "io_global_by_wait_by_bytes",
        "io_global_by_wait_by_latency",

        // InnoDB buffer pool views
        "innodb_buffer_stats_by_schema",
        "innodb_buffer_stats_by_table",
        "innodb_lock_waits",

        // Memory views
        "memory_by_host_by_current_bytes",
        "memory_by_thread_by_current_bytes",
        "memory_by_user_by_current_bytes",
        "memory_global_by_current_bytes",
        "memory_global_total",

        // Process views
        "processlist",
        "session",
        "session_ssl_status",

        // Schema views
        "schema_auto_increment_columns",
        "schema_index_statistics",
        "schema_object_overview",
        "schema_redundant_indexes",
        "schema_table_lock_waits",
        "schema_table_statistics",
        "schema_table_statistics_with_buffer",
        "schema_tables_with_full_table_scans",
        "schema_unused_indexes",

        // Statement views
        "statement_analysis",
        "statements_with_errors_or_warnings",
        "statements_with_full_table_scans",
        "statements_with_runtimes_in_95th_percentile",
        "statements_with_sorting",
        "statements_with_temp_tables",

        // User views
        "user_summary",
        "user_summary_by_file_io",
        "user_summary_by_file_io_type",
        "user_summary_by_stages",
        "user_summary_by_statement_latency",
        "user_summary_by_statement_type",

        // Wait views
        "wait_classes_global_by_avg_latency",
        "wait_classes_global_by_latency",
        "waits_by_host_by_latency",
        "waits_by_user_by_latency",
        "waits_global_by_latency",

        // Misc
        "metrics",
        "ps_check_lost_instrumentation",
        "version",
    ];

    public SysSchemaProvider(PerformanceSchemaProvider perfSchemaProvider)
    {
        _perfSchema = perfSchemaProvider;
    }

    /// <summary>
    /// Checks if a table/view name belongs to sys schema.
    /// </summary>
    public static bool IsSysSchemaView(string viewName)
    {
        return SupportedViews.Any(v => v.Equals(viewName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Queries a sys schema view.
    /// </summary>
    public ResultSet Query(string viewName)
    {
        return viewName.ToLowerInvariant() switch
        {
            "version" => QueryVersion(),
            "processlist" or "session" => QueryProcesslist(),
            "metrics" => QueryMetrics(),
            "memory_global_total" => QueryMemoryGlobalTotal(),
            "innodb_lock_waits" => QueryInnodbLockWaits(),
            "statement_analysis" => QueryStatementAnalysis(),
            "schema_table_statistics" => QuerySchemaTableStatistics(),
            "schema_index_statistics" => QuerySchemaIndexStatistics(),
            "schema_object_overview" => QuerySchemaObjectOverview(),
            "schema_unused_indexes" => QuerySchemaUnusedIndexes(),
            "user_summary" => QueryUserSummary(),
            "host_summary" => QueryHostSummary(),
            "waits_global_by_latency" => QueryWaitsGlobalByLatency(),
            _ => QueryEmptySysView(viewName)
        };
    }

    private ResultSet QueryVersion()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "sys_version", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "mysql_version", DataType = DataType.VarChar });
        rs.Rows.Add([
            DataValue.FromVarChar("2.1.2"),
            DataValue.FromVarChar(Constants.ServerVersion)
        ]);
        return rs;
    }

    private ResultSet QueryProcesslist()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "thd_id", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "conn_id", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "user", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "command", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "state", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "time", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "current_statement", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "lock_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_examined", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "rows_sent", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "tmp_tables", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "tmp_disk_tables", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "full_scan", DataType = DataType.VarChar });
        rs.Rows.Add([
            DataValue.FromBigInt(2), DataValue.FromBigInt(1),
            DataValue.FromVarChar("root@localhost"), DataValue.Null,
            DataValue.FromVarChar("Query"), DataValue.FromVarChar("executing"),
            DataValue.FromBigInt(0), DataValue.FromVarChar("SELECT * FROM sys.processlist"),
            DataValue.FromVarChar("0 ps"), DataValue.FromBigInt(0),
            DataValue.FromBigInt(0), DataValue.FromBigInt(0),
            DataValue.FromBigInt(0), DataValue.FromVarChar("NO")
        ]);
        return rs;
    }

    private ResultSet QueryMetrics()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "Variable_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Variable_value", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Type", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Enabled", DataType = DataType.VarChar });

        var uptime = (long)(DateTime.UtcNow - System.Diagnostics.Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalSeconds;
        var metrics = new (string name, string value, string type)[]
        {
            ("uptime", uptime.ToString(), "Global Status"),
            ("threads_connected", "1", "Global Status"),
            ("threads_running", "1", "Global Status"),
            ("innodb_buffer_pool_pages_total", "8192", "Global Status"),
            ("innodb_buffer_pool_pages_free", "8191", "Global Status"),
            ("innodb_buffer_pool_size", "134217728", "Global Variable"),
        };
        foreach (var (name, value, type) in metrics)
        {
            rs.Rows.Add([
                DataValue.FromVarChar(name), DataValue.FromVarChar(value),
                DataValue.FromVarChar(type), DataValue.FromVarChar("YES")
            ]);
        }
        return rs;
    }

    private ResultSet QueryMemoryGlobalTotal()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "total_allocated", DataType = DataType.VarChar });
        var mem = System.Diagnostics.Process.GetCurrentProcess().WorkingSet64;
        rs.Rows.Add([DataValue.FromVarChar(FormatBytes(mem))]);
        return rs;
    }

    private ResultSet QueryInnodbLockWaits()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "wait_started", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "wait_age", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "locked_table", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "locked_index", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "locked_type", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "waiting_trx_id", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "waiting_pid", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "waiting_query", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "blocking_trx_id", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "blocking_pid", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "blocking_query", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryStatementAnalysis()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "query", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "full_scan", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "exec_count", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "err_count", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "warn_count", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "total_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "max_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "avg_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_sent", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "rows_sent_avg", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "rows_examined", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "rows_examined_avg", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "first_seen", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "last_seen", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "digest", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QuerySchemaTableStatistics()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "table_schema", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "table_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "total_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_fetched", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "fetch_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_inserted", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "insert_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_updated", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "update_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_deleted", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "delete_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "io_read_requests", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "io_read", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "io_read_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "io_write_requests", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "io_write", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "io_write_latency", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QuerySchemaIndexStatistics()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "table_schema", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "table_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "index_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_selected", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "select_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_inserted", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "insert_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_updated", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "update_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_deleted", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "delete_latency", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QuerySchemaObjectOverview()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "object_type", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "count", DataType = DataType.BigInt });
        return rs;
    }

    private ResultSet QuerySchemaUnusedIndexes()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "object_schema", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "object_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "index_name", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryUserSummary()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "user", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "statements", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "statement_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "statement_avg_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "table_scans", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "file_ios", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "file_io_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "current_connections", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "total_connections", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "unique_hosts", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "current_memory", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "total_memory_allocated", DataType = DataType.VarChar });
        rs.Rows.Add([
            DataValue.FromVarChar("root"), DataValue.FromBigInt(0),
            DataValue.FromVarChar("0 ps"), DataValue.FromVarChar("0 ps"),
            DataValue.FromBigInt(0), DataValue.FromBigInt(0),
            DataValue.FromVarChar("0 ps"), DataValue.FromBigInt(1),
            DataValue.FromBigInt(1), DataValue.FromBigInt(1),
            DataValue.FromVarChar("0 bytes"), DataValue.FromVarChar("0 bytes")
        ]);
        return rs;
    }

    private ResultSet QueryHostSummary()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "host", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "statements", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "statement_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "statement_avg_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "table_scans", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "file_ios", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "file_io_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "current_connections", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "total_connections", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "unique_users", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "current_memory", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "total_memory_allocated", DataType = DataType.VarChar });
        rs.Rows.Add([
            DataValue.FromVarChar("localhost"), DataValue.FromBigInt(0),
            DataValue.FromVarChar("0 ps"), DataValue.FromVarChar("0 ps"),
            DataValue.FromBigInt(0), DataValue.FromBigInt(0),
            DataValue.FromVarChar("0 ps"), DataValue.FromBigInt(1),
            DataValue.FromBigInt(1), DataValue.FromBigInt(1),
            DataValue.FromVarChar("0 bytes"), DataValue.FromVarChar("0 bytes")
        ]);
        return rs;
    }

    private ResultSet QueryWaitsGlobalByLatency()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "events", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "total", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "total_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "avg_latency", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "max_latency", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryEmptySysView(string viewName)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "info", DataType = DataType.VarChar });
        return rs;
    }

    /// <summary>
    /// Formats bytes into a human-readable string (e.g., "128.00 MiB").
    /// </summary>
    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} bytes";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F2} KiB";
        if (bytes < 1024L * 1024 * 1024) return $"{bytes / (1024.0 * 1024):F2} MiB";
        return $"{bytes / (1024.0 * 1024 * 1024):F2} GiB";
    }
}
