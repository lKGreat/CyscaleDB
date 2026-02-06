using CyscaleDB.Core.Auth;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;

namespace CyscaleDB.Core.Storage.MysqlSchema;

/// <summary>
/// Provides virtual tables for the mysql system database.
/// Compatible with MySQL 8.4 system tables that third-party tools query.
/// </summary>
public sealed class MysqlSchemaProvider
{
    private readonly Catalog _catalog;

    public const string DatabaseName = "mysql";

    public static readonly string[] SupportedTables =
    [
        "user",
        "db",
        "tables_priv",
        "columns_priv",
        "procs_priv",
        "proxies_priv",
        "global_grants",
        "role_edges",
        "default_roles",
        "password_history",
        "func",
        "proc",
        "event",
        "plugin",
        "servers",
        "time_zone",
        "time_zone_name",
        "help_topic",
        "help_category",
        "help_keyword",
        "help_relation",
        "general_log",
        "slow_log",
        "innodb_table_stats",
        "innodb_index_stats",
        "engine_cost",
        "server_cost",
        "gtid_executed",
        "slave_relay_log_info",
        "slave_master_info",
        "slave_worker_info",
        "component",
    ];

    public MysqlSchemaProvider(Catalog catalog)
    {
        _catalog = catalog;
    }

    public static bool IsMysqlSchemaTable(string tableName)
    {
        return SupportedTables.Any(t => t.Equals(tableName, StringComparison.OrdinalIgnoreCase));
    }

    public ResultSet Query(string tableName)
    {
        return tableName.ToLowerInvariant() switch
        {
            "user" => QueryUser(),
            "db" => QueryDb(),
            "tables_priv" => QueryTablesPriv(),
            "columns_priv" => QueryColumnsPriv(),
            "procs_priv" => QueryEmpty("procs_priv"),
            "proc" => QueryProc(),
            "func" => QueryFunc(),
            "event" => QueryEvent(),
            "plugin" => QueryPlugin(),
            "role_edges" => QueryRoleEdges(),
            "default_roles" => QueryDefaultRoles(),
            "global_grants" => QueryGlobalGrants(),
            "general_log" => QueryGeneralLog(),
            "slow_log" => QuerySlowLog(),
            "innodb_table_stats" => QueryInnodbTableStats(),
            "innodb_index_stats" => QueryInnodbIndexStats(),
            "engine_cost" => QueryEngineCost(),
            "server_cost" => QueryServerCost(),
            "gtid_executed" => QueryGtidExecuted(),
            _ => QueryEmpty(tableName)
        };
    }

    private ResultSet QueryUser()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "Host", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "User", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Select_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Insert_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Update_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Delete_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Drop_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Reload_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Shutdown_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Process_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "File_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Grant_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "References_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Index_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Alter_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Show_db_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Super_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_tmp_table_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Lock_tables_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Execute_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Repl_slave_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Repl_client_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_view_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Show_view_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_routine_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Alter_routine_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_user_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Event_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Trigger_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_tablespace_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ssl_type", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "ssl_cipher", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "x509_issuer", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "x509_subject", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "max_questions", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "max_updates", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "max_connections", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "max_user_connections", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "plugin", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "authentication_string", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "password_expired", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "account_locked", DataType = DataType.VarChar });

        // Populate from UserManager
        try
        {
            var users = UserManager.Instance.GetAllUsers().Select(u => u.username);
            foreach (var user in users)
            {
                var allPrivs = "Y"; // root gets all
                rs.Rows.Add([
                    DataValue.FromVarChar("%"), DataValue.FromVarChar(user),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs), DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(allPrivs),
                    DataValue.FromVarChar(""), DataValue.FromVarChar(""),
                    DataValue.FromVarChar(""), DataValue.FromVarChar(""),
                    DataValue.FromInt(0), DataValue.FromInt(0),
                    DataValue.FromInt(0), DataValue.FromInt(0),
                    DataValue.FromVarChar("mysql_native_password"),
                    DataValue.FromVarChar(""),
                    DataValue.FromVarChar("N"), DataValue.FromVarChar("N")
                ]);
            }
        }
        catch
        {
            // Default root user if UserManager fails
            var y = DataValue.FromVarChar("Y");
            var e = DataValue.FromVarChar("");
            var row = new DataValue[43];
            row[0] = DataValue.FromVarChar("%");
            row[1] = DataValue.FromVarChar("root");
            for (int i = 2; i <= 31; i++) row[i] = y;
            for (int i = 32; i <= 34; i++) row[i] = e;
            row[35] = DataValue.FromInt(0); row[36] = DataValue.FromInt(0);
            row[37] = DataValue.FromInt(0); row[38] = DataValue.FromInt(0);
            row[39] = DataValue.FromVarChar("mysql_native_password");
            row[40] = e;
            row[41] = DataValue.FromVarChar("N"); row[42] = DataValue.FromVarChar("N");
            rs.Rows.Add(row);
        }
        return rs;
    }

    private ResultSet QueryDb()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "Host", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "User", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Select_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Insert_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Update_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Delete_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Drop_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Grant_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "References_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Index_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Alter_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_tmp_table_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Lock_tables_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_view_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Show_view_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Create_routine_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Alter_routine_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Execute_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Event_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Trigger_priv", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryTablesPriv()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "Host", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "User", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Table_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Grantor", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Timestamp", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "Table_priv", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Column_priv", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryColumnsPriv()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "Host", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "User", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Table_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Column_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Timestamp", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "Column_priv", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryProc()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "type", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "specific_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "language", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "sql_data_access", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "is_deterministic", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "security_type", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "definer", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "body", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "comment", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryFunc() => QueryProc();

    private ResultSet QueryEvent()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "body", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "definer", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "execute_at", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "interval_value", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "interval_field", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "status", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryPlugin()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "dl", DataType = DataType.VarChar });
        rs.Rows.Add([DataValue.FromVarChar("mysql_native_password"), DataValue.FromVarChar("")]);
        rs.Rows.Add([DataValue.FromVarChar("caching_sha2_password"), DataValue.FromVarChar("")]);
        return rs;
    }

    private ResultSet QueryRoleEdges()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "FROM_HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "FROM_USER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TO_HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TO_USER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "WITH_ADMIN_OPTION", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryDefaultRoles()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "USER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DEFAULT_ROLE_HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DEFAULT_ROLE_USER", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryGlobalGrants()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "USER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "HOST", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PRIV", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "WITH_GRANT_OPTION", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryGeneralLog()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "event_time", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "user_host", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "thread_id", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "server_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "command_type", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "argument", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QuerySlowLog()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "start_time", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "user_host", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "query_time", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "lock_time", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "rows_sent", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "rows_examined", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "db", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "sql_text", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryInnodbTableStats()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "database_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "table_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "last_update", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "n_rows", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "clustered_index_size", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "sum_of_other_index_sizes", DataType = DataType.BigInt });
        return rs;
    }

    private ResultSet QueryInnodbIndexStats()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "database_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "table_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "index_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "last_update", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "stat_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "stat_value", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "sample_size", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "stat_description", DataType = DataType.VarChar });
        return rs;
    }

    private ResultSet QueryEngineCost()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "engine_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "device_type", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "cost_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "cost_value", DataType = DataType.Double });
        rs.Columns.Add(new ResultColumn { Name = "last_update", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "comment", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "default_value", DataType = DataType.Double });
        return rs;
    }

    private ResultSet QueryServerCost()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "cost_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "cost_value", DataType = DataType.Double });
        rs.Columns.Add(new ResultColumn { Name = "last_update", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "comment", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "default_value", DataType = DataType.Double });
        var costs = new[] {
            ("disk_temptable_create_cost", 20.0),
            ("disk_temptable_row_cost", 0.5),
            ("key_compare_cost", 0.05),
            ("memory_temptable_create_cost", 1.0),
            ("memory_temptable_row_cost", 0.1),
            ("row_evaluate_cost", 0.1),
        };
        foreach (var (name, val) in costs)
        {
            rs.Rows.Add([
                DataValue.FromVarChar(name), DataValue.FromDouble(val),
                DataValue.FromDateTime(DateTime.UtcNow), DataValue.Null,
                DataValue.FromDouble(val)
            ]);
        }
        return rs;
    }

    private ResultSet QueryGtidExecuted()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "source_uuid", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "interval_start", DataType = DataType.BigInt });
        rs.Columns.Add(new ResultColumn { Name = "interval_end", DataType = DataType.BigInt });
        return rs;
    }

    private ResultSet QueryEmpty(string tableName)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "placeholder", DataType = DataType.VarChar });
        return rs;
    }
}
