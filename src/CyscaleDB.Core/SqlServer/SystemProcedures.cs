using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.SqlServer;

/// <summary>
/// Implements SQL Server sp_* system stored procedures.
/// These are called by SSMS and other tools to discover metadata.
/// </summary>
public static class SystemProcedures
{
    /// <summary>
    /// Tries to handle a system stored procedure call.
    /// Returns null if the query is not a system procedure call.
    /// </summary>
    public static ResultSet? TryHandleSystemProc(string sql, Catalog catalog, string currentDatabase)
    {
        var trimmed = sql.Trim().TrimEnd(';');

        if (trimmed.StartsWith("sp_databases", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("EXEC sp_databases", StringComparison.OrdinalIgnoreCase))
            return SpDatabases(catalog);

        if (trimmed.StartsWith("sp_tables", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("EXEC sp_tables", StringComparison.OrdinalIgnoreCase))
            return SpTables(catalog, currentDatabase);

        if (trimmed.StartsWith("sp_columns", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("EXEC sp_columns", StringComparison.OrdinalIgnoreCase))
        {
            var tableName = ExtractParam(trimmed, "table_name");
            if (tableName != null)
                return SpColumns(catalog, currentDatabase, tableName);
        }

        if (trimmed.StartsWith("sp_helpdb", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("EXEC sp_helpdb", StringComparison.OrdinalIgnoreCase))
            return SpHelpDb(catalog);

        if (trimmed.StartsWith("sp_who", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("EXEC sp_who", StringComparison.OrdinalIgnoreCase))
            return SpWho();

        if (trimmed.StartsWith("sp_help ", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("EXEC sp_help ", StringComparison.OrdinalIgnoreCase))
        {
            var tableName = ExtractFirstArg(trimmed);
            if (tableName != null)
                return SpHelp(catalog, currentDatabase, tableName);
        }

        if (trimmed.StartsWith("sp_server_info", StringComparison.OrdinalIgnoreCase) ||
            trimmed.StartsWith("EXEC sp_server_info", StringComparison.OrdinalIgnoreCase))
            return SpServerInfo();

        return null;
    }

    #region sp_databases

    private static ResultSet SpDatabases(Catalog catalog)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "DATABASE_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DATABASE_SIZE", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "REMARKS", DataType = DataType.VarChar });

        foreach (var db in catalog.Databases)
        {
            rs.Rows.Add([
                DataValue.FromVarChar(db.Name),
                DataValue.FromInt(0),
                DataValue.Null
            ]);
        }

        return rs;
    }

    #endregion

    #region sp_tables

    private static ResultSet SpTables(Catalog catalog, string currentDatabase)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "TABLE_QUALIFIER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TABLE_OWNER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TABLE_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TABLE_TYPE", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "REMARKS", DataType = DataType.VarChar });

        var db = catalog.GetDatabase(currentDatabase);
        if (db != null)
        {
            foreach (var table in db.Tables)
            {
                rs.Rows.Add([
                    DataValue.FromVarChar(currentDatabase),
                    DataValue.FromVarChar("dbo"),
                    DataValue.FromVarChar(table.TableName),
                    DataValue.FromVarChar("TABLE"),
                    DataValue.Null
                ]);
            }
        }

        return rs;
    }

    #endregion

    #region sp_columns

    private static ResultSet SpColumns(Catalog catalog, string currentDatabase, string tableName)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "TABLE_QUALIFIER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TABLE_OWNER", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "TABLE_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "COLUMN_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "DATA_TYPE", DataType = DataType.SmallInt });
        rs.Columns.Add(new ResultColumn { Name = "TYPE_NAME", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "PRECISION", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "LENGTH", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "SCALE", DataType = DataType.SmallInt });
        rs.Columns.Add(new ResultColumn { Name = "NULLABLE", DataType = DataType.SmallInt });

        var db = catalog.GetDatabase(currentDatabase);
        var table = db?.GetTable(tableName);
        if (table != null)
        {
            foreach (var col in table.Columns)
            {
                rs.Rows.Add([
                    DataValue.FromVarChar(currentDatabase),
                    DataValue.FromVarChar("dbo"),
                    DataValue.FromVarChar(tableName),
                    DataValue.FromVarChar(col.Name),
                    DataValue.FromSmallInt(MapOdbcDataType(col.DataType)),
                    DataValue.FromVarChar(col.DataType.ToString().ToLower()),
                    DataValue.FromInt(col.Precision > 0 ? col.Precision : col.MaxLength),
                    DataValue.FromInt(col.MaxLength),
                    DataValue.FromSmallInt((short)col.Scale),
                    DataValue.FromSmallInt(col.IsNullable ? (short)1 : (short)0)
                ]);
            }
        }

        return rs;
    }

    #endregion

    #region sp_helpdb

    private static ResultSet SpHelpDb(Catalog catalog)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "db_size", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "owner", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "dbid", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "created", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "status", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "compatibility_level", DataType = DataType.Int });

        int dbId = 1;
        foreach (var db in catalog.Databases)
        {
            rs.Rows.Add([
                DataValue.FromVarChar(db.Name),
                DataValue.FromVarChar("N/A"),
                DataValue.FromVarChar("sa"),
                DataValue.FromInt(dbId++),
                DataValue.FromVarChar(DateTime.Now.ToString("MMM dd yyyy")),
                DataValue.FromVarChar("Status=ONLINE"),
                DataValue.FromInt(160)
            ]);
        }

        return rs;
    }

    #endregion

    #region sp_who

    private static ResultSet SpWho()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "spid", DataType = DataType.SmallInt });
        rs.Columns.Add(new ResultColumn { Name = "ecid", DataType = DataType.SmallInt });
        rs.Columns.Add(new ResultColumn { Name = "status", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "loginame", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "hostname", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "blk", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "dbname", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "cmd", DataType = DataType.VarChar });

        // Return at least one row for the current system process
        rs.Rows.Add([
            DataValue.FromSmallInt(1), DataValue.FromSmallInt(0),
            DataValue.FromVarChar("sleeping"), DataValue.FromVarChar("sa"),
            DataValue.FromVarChar("CyscaleDB"), DataValue.FromVarChar("  ."),
            DataValue.FromVarChar("master"), DataValue.FromVarChar("AWAITING COMMAND")
        ]);

        return rs;
    }

    #endregion

    #region sp_help

    private static ResultSet SpHelp(Catalog catalog, string currentDatabase, string tableName)
    {
        // Return basic table info similar to SQL Server's sp_help
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "Name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Owner", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Type", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "Created_datetime", DataType = DataType.DateTime });

        var db = catalog.GetDatabase(currentDatabase);
        var table = db?.GetTable(tableName);
        if (table != null)
        {
            rs.Rows.Add([
                DataValue.FromVarChar(tableName),
                DataValue.FromVarChar("dbo"),
                DataValue.FromVarChar("user table"),
                DataValue.FromDateTime(DateTime.Now)
            ]);
        }

        return rs;
    }

    #endregion

    #region sp_server_info

    private static ResultSet SpServerInfo()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "attribute_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "attribute_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "attribute_value", DataType = DataType.VarChar });

        rs.Rows.Add([DataValue.FromInt(1), DataValue.FromVarChar("DBMS_NAME"), DataValue.FromVarChar("CyscaleDB (SQL Server Compatible)")]);
        rs.Rows.Add([DataValue.FromInt(2), DataValue.FromVarChar("DBMS_VER"), DataValue.FromVarChar("16.00.1000")]);
        rs.Rows.Add([DataValue.FromInt(10), DataValue.FromVarChar("OWNER_TERM"), DataValue.FromVarChar("owner")]);
        rs.Rows.Add([DataValue.FromInt(11), DataValue.FromVarChar("TABLE_TERM"), DataValue.FromVarChar("table")]);
        rs.Rows.Add([DataValue.FromInt(18), DataValue.FromVarChar("COLUMN_LENGTH"), DataValue.FromVarChar("8000")]);

        return rs;
    }

    #endregion

    #region Helpers

    private static string? ExtractParam(string sql, string paramName)
    {
        var pattern = $"@{paramName}\\s*=\\s*'([^']+)'";
        var match = System.Text.RegularExpressions.Regex.Match(sql, pattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        return match.Success ? match.Groups[1].Value : null;
    }

    private static string? ExtractFirstArg(string sql)
    {
        // Extract first argument after procedure name: sp_help 'tablename' or sp_help tablename
        var parts = sql.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length >= 2)
        {
            var arg = parts[^1].Trim('\'', '"', '[', ']', '`');
            return string.IsNullOrEmpty(arg) ? null : arg;
        }
        return null;
    }

    private static short MapOdbcDataType(DataType type)
    {
        return type switch
        {
            DataType.Int => 4,         // SQL_INTEGER
            DataType.BigInt => -5,     // SQL_BIGINT
            DataType.SmallInt => 5,    // SQL_SMALLINT
            DataType.TinyInt => -6,    // SQL_TINYINT
            DataType.Boolean => -7,    // SQL_BIT
            DataType.Float => 7,       // SQL_REAL
            DataType.Double => 8,      // SQL_DOUBLE
            DataType.Decimal => 3,     // SQL_DECIMAL
            DataType.DateTime => 93,   // SQL_TYPE_TIMESTAMP
            DataType.Date => 91,       // SQL_TYPE_DATE
            DataType.Time => 92,       // SQL_TYPE_TIME
            DataType.VarChar => 12,    // SQL_VARCHAR
            DataType.Char => 1,        // SQL_CHAR
            DataType.Text => -1,       // SQL_LONGVARCHAR
            DataType.Blob => -4,       // SQL_LONGVARBINARY
            _ => 12                    // default to SQL_VARCHAR
        };
    }

    #endregion
}
