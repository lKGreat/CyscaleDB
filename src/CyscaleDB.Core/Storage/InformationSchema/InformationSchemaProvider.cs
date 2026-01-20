using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;

namespace CyscaleDB.Core.Storage.InformationSchema;

/// <summary>
/// Provides virtual tables for the information_schema database.
/// These tables return metadata about databases, tables, columns, etc.
/// </summary>
public sealed class InformationSchemaProvider
{
    private readonly Catalog _catalog;

    /// <summary>
    /// The name of the information_schema database.
    /// </summary>
    public const string DatabaseName = "information_schema";

    /// <summary>
    /// List of supported virtual tables in information_schema.
    /// </summary>
    public static readonly string[] SupportedTables =
    [
        "SCHEMATA",
        "TABLES",
        "COLUMNS",
        "STATISTICS",
        "ENGINES",
        "ROUTINES",
        "FILES",
        "KEY_COLUMN_USAGE",
        "REFERENTIAL_CONSTRAINTS"
    ];

    public InformationSchemaProvider(Catalog catalog)
    {
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    /// <summary>
    /// Checks if a table name is a valid information_schema table.
    /// </summary>
    public static bool IsValidTable(string tableName)
    {
        return SupportedTables.Contains(tableName, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Gets data for the specified information_schema table.
    /// </summary>
    public ResultSet GetTableData(string tableName, string? filterSchema = null, string? filterTable = null)
    {
        return tableName.ToUpperInvariant() switch
        {
            "SCHEMATA" => GetSchemata(filterSchema),
            "TABLES" => GetTables(filterSchema, filterTable),
            "COLUMNS" => GetColumns(filterSchema, filterTable),
            "STATISTICS" => GetStatistics(filterSchema, filterTable),
            "ENGINES" => GetEngines(),
            "ROUTINES" => GetRoutines(filterSchema),
            "FILES" => GetFiles(),
            "KEY_COLUMN_USAGE" => GetKeyColumnUsage(filterSchema, filterTable),
            "REFERENTIAL_CONSTRAINTS" => GetReferentialConstraints(filterSchema, filterTable),
            _ => throw new CyscaleException($"Unknown information_schema table: {tableName}")
        };
    }

    /// <summary>
    /// Returns schema for the specified information_schema table.
    /// </summary>
    public static TableSchema GetTableSchema(string tableName)
    {
        return tableName.ToUpperInvariant() switch
        {
            "SCHEMATA" => CreateSchemataSchema(),
            "TABLES" => CreateTablesSchema(),
            "COLUMNS" => CreateColumnsSchema(),
            "STATISTICS" => CreateStatisticsSchema(),
            "ENGINES" => CreateEnginesSchema(),
            "ROUTINES" => CreateRoutinesSchema(),
            "FILES" => CreateFilesSchema(),
            "KEY_COLUMN_USAGE" => CreateKeyColumnUsageSchema(),
            "REFERENTIAL_CONSTRAINTS" => CreateReferentialConstraintsSchema(),
            _ => throw new CyscaleException($"Unknown information_schema table: {tableName}")
        };
    }

    #region SCHEMATA

    private static TableSchema CreateSchemataSchema()
    {
        return new TableSchema(0, DatabaseName, "SCHEMATA",
        [
            new ColumnDefinition("CATALOG_NAME", DataType.VarChar, 64),
            new ColumnDefinition("SCHEMA_NAME", DataType.VarChar, 64),
            new ColumnDefinition("DEFAULT_CHARACTER_SET_NAME", DataType.VarChar, 64),
            new ColumnDefinition("DEFAULT_COLLATION_NAME", DataType.VarChar, 64),
            new ColumnDefinition("SQL_PATH", DataType.VarChar, 512)
        ]);
    }

    private ResultSet GetSchemata(string? filterSchema)
    {
        var result = ResultSet.FromSchema(CreateSchemataSchema());

        foreach (var db in _catalog.Databases)
        {
            if (filterSchema != null && !db.Name.Equals(filterSchema, StringComparison.OrdinalIgnoreCase))
                continue;

            result.Rows.Add([
                DataValue.FromVarChar("def"),
                DataValue.FromVarChar(db.Name),
                DataValue.FromVarChar(db.CharacterSet),
                DataValue.FromVarChar(db.Collation),
                DataValue.Null
            ]);
        }

        // Add information_schema itself
        if (filterSchema == null || filterSchema.Equals(DatabaseName, StringComparison.OrdinalIgnoreCase))
        {
            result.Rows.Add([
                DataValue.FromVarChar("def"),
                DataValue.FromVarChar(DatabaseName),
                DataValue.FromVarChar("utf8mb4"),
                DataValue.FromVarChar("utf8mb4_general_ci"),
                DataValue.Null
            ]);
        }

        return result;
    }

    #endregion

    #region TABLES

    private static TableSchema CreateTablesSchema()
    {
        return new TableSchema(0, DatabaseName, "TABLES",
        [
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("ENGINE", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_COLLATION", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_COMMENT", DataType.Text)
        ]);
    }

    private ResultSet GetTables(string? filterSchema, string? filterTable)
    {
        var result = ResultSet.FromSchema(CreateTablesSchema());

        foreach (var db in _catalog.Databases)
        {
            if (filterSchema != null && !db.Name.Equals(filterSchema, StringComparison.OrdinalIgnoreCase))
                continue;

            foreach (var table in db.Tables)
            {
                if (filterTable != null && !table.TableName.Equals(filterTable, StringComparison.OrdinalIgnoreCase))
                    continue;

                result.Rows.Add([
                    DataValue.FromVarChar("def"),
                    DataValue.FromVarChar(db.Name),
                    DataValue.FromVarChar(table.TableName),
                    DataValue.FromVarChar("BASE TABLE"),
                    DataValue.FromVarChar("CyscaleDB"),
                    DataValue.FromVarChar("utf8mb4_general_ci"),
                    DataValue.FromVarChar("")
                ]);
            }

            // Include views
            foreach (var view in db.Views)
            {
                if (filterTable != null && !view.ViewName.Equals(filterTable, StringComparison.OrdinalIgnoreCase))
                    continue;

                result.Rows.Add([
                    DataValue.FromVarChar("def"),
                    DataValue.FromVarChar(db.Name),
                    DataValue.FromVarChar(view.ViewName),
                    DataValue.FromVarChar("VIEW"),
                    DataValue.Null,
                    DataValue.Null,
                    DataValue.FromVarChar("VIEW")
                ]);
            }
        }

        return result;
    }

    #endregion

    #region COLUMNS

    private static TableSchema CreateColumnsSchema()
    {
        return new TableSchema(0, DatabaseName, "COLUMNS",
        [
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLUMN_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ORDINAL_POSITION", DataType.Int),
            new ColumnDefinition("COLUMN_DEFAULT", DataType.Text),
            new ColumnDefinition("IS_NULLABLE", DataType.VarChar, 3),
            new ColumnDefinition("DATA_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("COLUMN_TYPE", DataType.Text),
            new ColumnDefinition("COLUMN_KEY", DataType.VarChar, 3),
            new ColumnDefinition("EXTRA", DataType.VarChar, 256)
        ]);
    }

    private ResultSet GetColumns(string? filterSchema, string? filterTable)
    {
        var result = ResultSet.FromSchema(CreateColumnsSchema());

        foreach (var db in _catalog.Databases)
        {
            if (filterSchema != null && !db.Name.Equals(filterSchema, StringComparison.OrdinalIgnoreCase))
                continue;

            foreach (var table in db.Tables)
            {
                if (filterTable != null && !table.TableName.Equals(filterTable, StringComparison.OrdinalIgnoreCase))
                    continue;

                var ordinal = 1;
                foreach (var col in table.Columns)
                {
                    result.Rows.Add([
                        DataValue.FromVarChar("def"),
                        DataValue.FromVarChar(db.Name),
                        DataValue.FromVarChar(table.TableName),
                        DataValue.FromVarChar(col.Name),
                        DataValue.FromInt(ordinal++),
                        col.DefaultValue.HasValue ? DataValue.FromVarChar(col.DefaultValue.Value.ToString()) : DataValue.Null,
                        DataValue.FromVarChar(col.IsNullable ? "YES" : "NO"),
                        DataValue.FromVarChar(GetMySqlDataTypeName(col.DataType)),
                        DataValue.FromVarChar(GetColumnType(col)),
                        DataValue.FromVarChar(col.IsPrimaryKey ? "PRI" : ""),
                        DataValue.FromVarChar(col.IsAutoIncrement ? "auto_increment" : "")
                    ]);
                }
            }
        }

        return result;
    }

    #endregion

    #region STATISTICS (Indexes)

    private static TableSchema CreateStatisticsSchema()
    {
        return new TableSchema(0, DatabaseName, "STATISTICS",
        [
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("NON_UNIQUE", DataType.Int),
            new ColumnDefinition("INDEX_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("INDEX_NAME", DataType.VarChar, 64),
            new ColumnDefinition("SEQ_IN_INDEX", DataType.Int),
            new ColumnDefinition("COLUMN_NAME", DataType.VarChar, 64),
            new ColumnDefinition("INDEX_TYPE", DataType.VarChar, 16)
        ]);
    }

    private ResultSet GetStatistics(string? filterSchema, string? filterTable)
    {
        var result = ResultSet.FromSchema(CreateStatisticsSchema());

        foreach (var db in _catalog.Databases)
        {
            if (filterSchema != null && !db.Name.Equals(filterSchema, StringComparison.OrdinalIgnoreCase))
                continue;

            foreach (var table in db.Tables)
            {
                if (filterTable != null && !table.TableName.Equals(filterTable, StringComparison.OrdinalIgnoreCase))
                    continue;

                var seq = 1;
                foreach (var col in table.Columns)
                {
                    if (col.IsPrimaryKey)
                    {
                        result.Rows.Add([
                            DataValue.FromVarChar("def"),
                            DataValue.FromVarChar(db.Name),
                            DataValue.FromVarChar(table.TableName),
                            DataValue.FromInt(0), // NON_UNIQUE = 0 for primary key
                            DataValue.FromVarChar(db.Name),
                            DataValue.FromVarChar("PRIMARY"),
                            DataValue.FromInt(seq++),
                            DataValue.FromVarChar(col.Name),
                            DataValue.FromVarChar("BTREE")
                        ]);
                    }
                }
            }
        }

        return result;
    }

    #endregion

    #region ENGINES

    private static TableSchema CreateEnginesSchema()
    {
        return new TableSchema(0, DatabaseName, "ENGINES",
        [
            new ColumnDefinition("ENGINE", DataType.VarChar, 64),
            new ColumnDefinition("SUPPORT", DataType.VarChar, 8),
            new ColumnDefinition("COMMENT", DataType.VarChar, 160),
            new ColumnDefinition("TRANSACTIONS", DataType.VarChar, 3),
            new ColumnDefinition("XA", DataType.VarChar, 3),
            new ColumnDefinition("SAVEPOINTS", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetEngines()
    {
        var result = ResultSet.FromSchema(CreateEnginesSchema());

        // Return CyscaleDB as the default engine
        result.Rows.Add([
            DataValue.FromVarChar("CyscaleDB"),
            DataValue.FromVarChar("DEFAULT"),
            DataValue.FromVarChar("CyscaleDB native storage engine"),
            DataValue.FromVarChar("YES"),
            DataValue.FromVarChar("NO"),
            DataValue.FromVarChar("NO")
        ]);

        return result;
    }

    #endregion

    #region ROUTINES (Stored procedures/functions - stub)

    private static TableSchema CreateRoutinesSchema()
    {
        return new TableSchema(0, DatabaseName, "ROUTINES",
        [
            new ColumnDefinition("SPECIFIC_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ROUTINE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("ROUTINE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("ROUTINE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ROUTINE_TYPE", DataType.VarChar, 13),
            new ColumnDefinition("DATA_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("ROUTINE_BODY", DataType.VarChar, 8),
            new ColumnDefinition("ROUTINE_DEFINITION", DataType.Text),
            new ColumnDefinition("EXTERNAL_NAME", DataType.VarChar, 64),
            new ColumnDefinition("EXTERNAL_LANGUAGE", DataType.VarChar, 64),
            new ColumnDefinition("PARAMETER_STYLE", DataType.VarChar, 8),
            new ColumnDefinition("IS_DETERMINISTIC", DataType.VarChar, 3),
            new ColumnDefinition("SQL_DATA_ACCESS", DataType.VarChar, 64),
            new ColumnDefinition("SQL_PATH", DataType.VarChar, 64),
            new ColumnDefinition("SECURITY_TYPE", DataType.VarChar, 7),
            new ColumnDefinition("CREATED", DataType.DateTime),
            new ColumnDefinition("LAST_ALTERED", DataType.DateTime),
            new ColumnDefinition("SQL_MODE", DataType.VarChar, 8192),
            new ColumnDefinition("ROUTINE_COMMENT", DataType.Text),
            new ColumnDefinition("DEFINER", DataType.VarChar, 288),
            new ColumnDefinition("CHARACTER_SET_CLIENT", DataType.VarChar, 32),
            new ColumnDefinition("COLLATION_CONNECTION", DataType.VarChar, 32),
            new ColumnDefinition("DATABASE_COLLATION", DataType.VarChar, 32)
        ]);
    }

    private ResultSet GetRoutines(string? filterSchema)
    {
        // Return empty result - CyscaleDB doesn't support stored procedures yet
        return ResultSet.FromSchema(CreateRoutinesSchema());
    }

    #endregion

    #region FILES (Tablespace files - stub)

    private static TableSchema CreateFilesSchema()
    {
        return new TableSchema(0, DatabaseName, "FILES",
        [
            new ColumnDefinition("FILE_ID", DataType.BigInt),
            new ColumnDefinition("FILE_NAME", DataType.VarChar, 4000),
            new ColumnDefinition("FILE_TYPE", DataType.VarChar, 20),
            new ColumnDefinition("TABLESPACE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ENGINE", DataType.VarChar, 64),
            new ColumnDefinition("FULLTEXT_KEYS", DataType.VarChar, 64),
            new ColumnDefinition("STATUS", DataType.VarChar, 20),
            new ColumnDefinition("EXTRA", DataType.VarChar, 255)
        ]);
    }

    private ResultSet GetFiles()
    {
        // Return empty result - CyscaleDB doesn't support tablespace files
        return ResultSet.FromSchema(CreateFilesSchema());
    }

    #endregion

    #region KEY_COLUMN_USAGE (Foreign key information - stub)

    private static TableSchema CreateKeyColumnUsageSchema()
    {
        return new TableSchema(0, DatabaseName, "KEY_COLUMN_USAGE",
        [
            new ColumnDefinition("CONSTRAINT_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_NAME", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLUMN_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ORDINAL_POSITION", DataType.Int),
            new ColumnDefinition("POSITION_IN_UNIQUE_CONSTRAINT", DataType.Int),
            new ColumnDefinition("REFERENCED_TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("REFERENCED_TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("REFERENCED_COLUMN_NAME", DataType.VarChar, 64)
        ]);
    }

    private ResultSet GetKeyColumnUsage(string? filterSchema, string? filterTable)
    {
        // Return empty result - CyscaleDB doesn't track foreign keys in information_schema yet
        return ResultSet.FromSchema(CreateKeyColumnUsageSchema());
    }

    #endregion

    #region REFERENTIAL_CONSTRAINTS (Foreign key constraints - stub)

    private static TableSchema CreateReferentialConstraintsSchema()
    {
        return new TableSchema(0, DatabaseName, "REFERENTIAL_CONSTRAINTS",
        [
            new ColumnDefinition("CONSTRAINT_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_NAME", DataType.VarChar, 64),
            new ColumnDefinition("UNIQUE_CONSTRAINT_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("UNIQUE_CONSTRAINT_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("UNIQUE_CONSTRAINT_NAME", DataType.VarChar, 64),
            new ColumnDefinition("MATCH_OPTION", DataType.VarChar, 64),
            new ColumnDefinition("UPDATE_RULE", DataType.VarChar, 64),
            new ColumnDefinition("DELETE_RULE", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("REFERENCED_TABLE_NAME", DataType.VarChar, 64)
        ]);
    }

    private ResultSet GetReferentialConstraints(string? filterSchema, string? filterTable)
    {
        // Return empty result - CyscaleDB doesn't track foreign keys yet
        return ResultSet.FromSchema(CreateReferentialConstraintsSchema());
    }

    #endregion

    #region Helper Methods

    private static string GetMySqlDataTypeName(DataType dataType)
    {
        return dataType switch
        {
            DataType.Int => "int",
            DataType.BigInt => "bigint",
            DataType.SmallInt => "smallint",
            DataType.TinyInt => "tinyint",
            DataType.VarChar => "varchar",
            DataType.Char => "char",
            DataType.Text => "text",
            DataType.Boolean => "tinyint",
            DataType.DateTime => "datetime",
            DataType.Date => "date",
            DataType.Time => "time",
            DataType.Timestamp => "timestamp",
            DataType.Float => "float",
            DataType.Double => "double",
            DataType.Decimal => "decimal",
            DataType.Blob => "blob",
            _ => "varchar"
        };
    }

    private static string GetColumnType(ColumnDefinition col)
    {
        var baseName = GetMySqlDataTypeName(col.DataType);
        if (col.MaxLength > 0)
            return $"{baseName}({col.MaxLength})";
        if (col.DataType == DataType.Boolean)
            return "tinyint(1)";
        return baseName;
    }

    #endregion
}
