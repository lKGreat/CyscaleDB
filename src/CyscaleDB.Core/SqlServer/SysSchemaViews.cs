using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Core.SqlServer;

/// <summary>
/// Implements SQL Server sys.* system views by translating them into
/// queries against CyscaleDB's metadata (Catalog/Information_Schema).
/// These views are critical for SSMS Object Explorer functionality.
/// </summary>
public static class SysSchemaViews
{
    /// <summary>
    /// Tries to handle a query against sys.* views.
    /// Returns null if the query is not a sys.* query.
    /// </summary>
    public static ResultSet? TryHandleSysQuery(string sql, Catalog catalog, string currentDatabase)
    {
        var trimmed = sql.Trim().TrimEnd(';');

        // sys.databases
        if (ContainsView(trimmed, "sys.databases"))
            return GetSysDatabases(catalog);

        // sys.tables
        if (ContainsView(trimmed, "sys.tables"))
            return GetSysTables(catalog, currentDatabase);

        // sys.columns
        if (ContainsView(trimmed, "sys.columns"))
            return GetSysColumns(catalog, currentDatabase);

        // sys.objects
        if (ContainsView(trimmed, "sys.objects"))
            return GetSysObjects(catalog, currentDatabase);

        // sys.types
        if (ContainsView(trimmed, "sys.types"))
            return GetSysTypes();

        // sys.schemas
        if (ContainsView(trimmed, "sys.schemas"))
            return GetSysSchemas();

        // sys.indexes
        if (ContainsView(trimmed, "sys.indexes"))
            return GetSysIndexes(catalog, currentDatabase);

        // sys.database_files
        if (ContainsView(trimmed, "sys.database_files"))
            return GetSysDatabaseFiles(catalog, currentDatabase);

        // sys.filegroups
        if (ContainsView(trimmed, "sys.filegroups"))
            return GetSysFileGroups();

        // sys.configurations
        if (ContainsView(trimmed, "sys.configurations"))
            return GetSysConfigurations();

        // sys.server_principals
        if (ContainsView(trimmed, "sys.server_principals"))
            return GetSysServerPrincipals();

        return null;
    }

    private static bool ContainsView(string sql, string viewName)
    {
        return sql.Contains(viewName, StringComparison.OrdinalIgnoreCase);
    }

    #region sys.databases

    private static ResultSet GetSysDatabases(Catalog catalog)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "database_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "create_date", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "compatibility_level", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "collation_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "state", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "state_desc", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "recovery_model", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "recovery_model_desc", DataType = DataType.VarChar });

        int dbId = 1;

        // Add system databases
        rs.Rows.Add([
            DataValue.FromVarChar("master"), DataValue.FromInt(dbId++),
            DataValue.FromDateTime(DateTime.Now), DataValue.FromInt(160),
            DataValue.FromVarChar("SQL_Latin1_General_CP1_CI_AS"),
            DataValue.FromTinyInt(0), DataValue.FromVarChar("ONLINE"),
            DataValue.FromTinyInt(1), DataValue.FromVarChar("FULL")
        ]);

        // Add user databases
        foreach (var db in catalog.Databases)
        {
            if (db.Name == "information_schema" || db.Name == "performance_schema" || db.Name == "sys")
                continue;

            rs.Rows.Add([
                DataValue.FromVarChar(db.Name), DataValue.FromInt(dbId++),
                DataValue.FromDateTime(db.CreatedAt), DataValue.FromInt(160),
                DataValue.FromVarChar("SQL_Latin1_General_CP1_CI_AS"),
                DataValue.FromTinyInt(0), DataValue.FromVarChar("ONLINE"),
                DataValue.FromTinyInt(1), DataValue.FromVarChar("FULL")
            ]);
        }

        return rs;
    }

    #endregion

    #region sys.tables

    private static ResultSet GetSysTables(Catalog catalog, string currentDatabase)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "object_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "schema_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "type", DataType = DataType.Char });
        rs.Columns.Add(new ResultColumn { Name = "type_desc", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "create_date", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "modify_date", DataType = DataType.DateTime });

        var db = catalog.GetDatabase(currentDatabase);
        if (db != null)
        {
            int objectId = 1000;
            foreach (var table in db.Tables)
            {
                rs.Rows.Add([
                    DataValue.FromVarChar(table.TableName),
                    DataValue.FromInt(objectId++),
                    DataValue.FromInt(1), // dbo schema
                    DataValue.FromChar("U "),
                    DataValue.FromVarChar("USER_TABLE"),
                    DataValue.FromDateTime(DateTime.Now),
                    DataValue.FromDateTime(DateTime.Now)
                ]);
            }
        }

        return rs;
    }

    #endregion

    #region sys.columns

    private static ResultSet GetSysColumns(Catalog catalog, string currentDatabase)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "object_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "column_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "system_type_id", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "max_length", DataType = DataType.SmallInt });
        rs.Columns.Add(new ResultColumn { Name = "precision", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "scale", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "is_nullable", DataType = DataType.Boolean });
        rs.Columns.Add(new ResultColumn { Name = "is_identity", DataType = DataType.Boolean });

        var db = catalog.GetDatabase(currentDatabase);
        if (db != null)
        {
            int objectId = 1000;
            foreach (var table in db.Tables)
            {
                int colId = 1;
                foreach (var col in table.Columns)
                {
                    rs.Rows.Add([
                        DataValue.FromVarChar(col.Name),
                        DataValue.FromInt(objectId),
                        DataValue.FromInt(colId++),
                        DataValue.FromTinyInt(MapToSqlServerTypeId(col.DataType)),
                        DataValue.FromSmallInt((short)(col.MaxLength > 0 ? col.MaxLength : -1)),
                        DataValue.FromTinyInt((sbyte)col.Precision),
                        DataValue.FromTinyInt((sbyte)col.Scale),
                        DataValue.FromBoolean(col.IsNullable),
                        DataValue.FromBoolean(col.IsAutoIncrement)
                    ]);
                }
                objectId++;
            }
        }

        return rs;
    }

    #endregion

    #region sys.objects

    private static ResultSet GetSysObjects(Catalog catalog, string currentDatabase)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "object_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "schema_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "type", DataType = DataType.Char });
        rs.Columns.Add(new ResultColumn { Name = "type_desc", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "create_date", DataType = DataType.DateTime });
        rs.Columns.Add(new ResultColumn { Name = "modify_date", DataType = DataType.DateTime });

        var db = catalog.GetDatabase(currentDatabase);
        if (db != null)
        {
            int objectId = 1000;
            foreach (var table in db.Tables)
            {
                rs.Rows.Add([
                    DataValue.FromVarChar(table.TableName),
                    DataValue.FromInt(objectId++),
                    DataValue.FromInt(1),
                    DataValue.FromChar("U "),
                    DataValue.FromVarChar("USER_TABLE"),
                    DataValue.FromDateTime(DateTime.Now),
                    DataValue.FromDateTime(DateTime.Now)
                ]);
            }
        }

        return rs;
    }

    #endregion

    #region sys.types

    private static ResultSet GetSysTypes()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "system_type_id", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "user_type_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "max_length", DataType = DataType.SmallInt });
        rs.Columns.Add(new ResultColumn { Name = "precision", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "scale", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "is_nullable", DataType = DataType.Boolean });

        var types = new (string name, sbyte id, short maxLen, sbyte prec, sbyte scale)[]
        {
            ("int", 56, 4, 10, 0),
            ("bigint", 127, 8, 19, 0),
            ("smallint", 52, 2, 5, 0),
            ("tinyint", 48, 1, 3, 0),
            ("bit", 104, 1, 1, 0),
            ("decimal", 106, 17, 38, 0),
            ("numeric", 108, 17, 38, 0),
            ("float", 62, 8, 53, 0),
            ("real", 59, 4, 24, 0),
            ("datetime", 61, 8, 0, 0),
            ("varchar", 39, -1, 0, 0),
            ("nvarchar", 39, -1, 0, 0),
            ("text", 35, 16, 0, 0),
            ("ntext", 35, 16, 0, 0),
            ("varbinary", 37, -1, 0, 0),
        };

        foreach (var (name, id, maxLen, prec, scale) in types)
        {
            rs.Rows.Add([
                DataValue.FromVarChar(name),
                DataValue.FromTinyInt(id),
                DataValue.FromInt(id),
                DataValue.FromSmallInt(maxLen),
                DataValue.FromTinyInt(prec),
                DataValue.FromTinyInt(scale),
                DataValue.FromBoolean(true)
            ]);
        }

        return rs;
    }

    #endregion

    #region sys.schemas

    private static ResultSet GetSysSchemas()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "schema_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "principal_id", DataType = DataType.Int });

        rs.Rows.Add([DataValue.FromVarChar("dbo"), DataValue.FromInt(1), DataValue.FromInt(1)]);
        rs.Rows.Add([DataValue.FromVarChar("sys"), DataValue.FromInt(4), DataValue.FromInt(4)]);
        rs.Rows.Add([DataValue.FromVarChar("INFORMATION_SCHEMA"), DataValue.FromInt(3), DataValue.FromInt(3)]);

        return rs;
    }

    #endregion

    #region sys.indexes

    private static ResultSet GetSysIndexes(Catalog catalog, string currentDatabase)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "object_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "index_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "type", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "type_desc", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "is_unique", DataType = DataType.Boolean });
        rs.Columns.Add(new ResultColumn { Name = "is_primary_key", DataType = DataType.Boolean });

        var db = catalog.GetDatabase(currentDatabase);
        if (db != null)
        {
            int objectId = 1000;
            foreach (var table in db.Tables)
            {
                // Add clustered index (primary key)
                rs.Rows.Add([
                    DataValue.FromVarChar("PK_" + table.TableName),
                    DataValue.FromInt(objectId),
                    DataValue.FromInt(1),
                    DataValue.FromTinyInt(1), // CLUSTERED
                    DataValue.FromVarChar("CLUSTERED"),
                    DataValue.FromBoolean(true),
                    DataValue.FromBoolean(true)
                ]);
                objectId++;
            }
        }

        return rs;
    }

    #endregion

    #region sys.database_files / sys.filegroups

    private static ResultSet GetSysDatabaseFiles(Catalog catalog, string currentDatabase)
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "file_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "type", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "type_desc", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "physical_name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "state", DataType = DataType.TinyInt });
        rs.Columns.Add(new ResultColumn { Name = "state_desc", DataType = DataType.VarChar });

        rs.Rows.Add([
            DataValue.FromInt(1), DataValue.FromTinyInt(0), DataValue.FromVarChar("ROWS"),
            DataValue.FromVarChar(currentDatabase), DataValue.FromVarChar($"{currentDatabase}.cdb"),
            DataValue.FromTinyInt(0), DataValue.FromVarChar("ONLINE")
        ]);

        return rs;
    }

    private static ResultSet GetSysFileGroups()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "data_space_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "type", DataType = DataType.Char });
        rs.Columns.Add(new ResultColumn { Name = "is_default", DataType = DataType.Boolean });

        rs.Rows.Add([
            DataValue.FromVarChar("PRIMARY"), DataValue.FromInt(1),
            DataValue.FromChar("FG"), DataValue.FromBoolean(true)
        ]);

        return rs;
    }

    #endregion

    #region sys.configurations / sys.server_principals

    private static ResultSet GetSysConfigurations()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "configuration_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "value", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "minimum", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "maximum", DataType = DataType.Int });

        rs.Rows.Add([
            DataValue.FromInt(1), DataValue.FromVarChar("max degree of parallelism"),
            DataValue.FromInt(0), DataValue.FromInt(0), DataValue.FromInt(32767)
        ]);

        return rs;
    }

    private static ResultSet GetSysServerPrincipals()
    {
        var rs = new ResultSet();
        rs.Columns.Add(new ResultColumn { Name = "name", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "principal_id", DataType = DataType.Int });
        rs.Columns.Add(new ResultColumn { Name = "type", DataType = DataType.Char });
        rs.Columns.Add(new ResultColumn { Name = "type_desc", DataType = DataType.VarChar });
        rs.Columns.Add(new ResultColumn { Name = "is_disabled", DataType = DataType.Boolean });

        rs.Rows.Add([
            DataValue.FromVarChar("sa"), DataValue.FromInt(1),
            DataValue.FromChar("S"), DataValue.FromVarChar("SQL_LOGIN"),
            DataValue.FromBoolean(false)
        ]);

        return rs;
    }

    #endregion

    #region Helpers

    private static sbyte MapToSqlServerTypeId(DataType type)
    {
        return type switch
        {
            DataType.Int => 56,
            DataType.BigInt => 127,
            DataType.SmallInt => 52,
            DataType.TinyInt => 48,
            DataType.Boolean => 104,
            DataType.Float => 59,
            DataType.Double => 62,
            DataType.Decimal => 106,
            DataType.DateTime or DataType.Timestamp => 61,
            DataType.Date => 40,
            DataType.Time => 41,
            DataType.VarChar or DataType.Char => 39,
            DataType.Text => 35,
            DataType.Blob => 37,
            _ => 39 // default to varchar
        };
    }

    #endregion
}
