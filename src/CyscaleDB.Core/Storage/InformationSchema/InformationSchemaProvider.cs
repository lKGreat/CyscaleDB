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
        "ENGINES"
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
