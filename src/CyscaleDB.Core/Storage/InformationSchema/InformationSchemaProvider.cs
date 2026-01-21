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
        // Core metadata tables
        "SCHEMATA",
        "TABLES",
        "COLUMNS",
        "STATISTICS",
        "ENGINES",
        "VIEWS",
        
        // Character set and collation tables
        "CHARACTER_SETS",
        "COLLATIONS",
        "COLLATION_CHARACTER_SET_APPLICABILITY",
        
        // Constraint tables
        "TABLE_CONSTRAINTS",
        "CHECK_CONSTRAINTS",
        "KEY_COLUMN_USAGE",
        "REFERENTIAL_CONSTRAINTS",
        
        // System tables
        "PROCESSLIST",
        "PLUGINS",
        "EVENTS",
        "PARTITIONS",
        
        // Routine tables
        "ROUTINES",
        "PARAMETERS",
        "TRIGGERS",
        
        // Privilege tables
        "COLUMN_PRIVILEGES",
        "TABLE_PRIVILEGES",
        "SCHEMA_PRIVILEGES",
        "USER_PRIVILEGES",
        "USER_ATTRIBUTES",
        
        // Role tables
        "ADMINISTRABLE_ROLE_AUTHORIZATIONS",
        "APPLICABLE_ROLES",
        "ENABLED_ROLES",
        "ROLE_COLUMN_GRANTS",
        "ROLE_ROUTINE_GRANTS",
        "ROLE_TABLE_GRANTS",
        
        // Extension tables
        "SCHEMATA_EXTENSIONS",
        "TABLES_EXTENSIONS",
        "COLUMNS_EXTENSIONS",
        "TABLE_CONSTRAINTS_EXTENSIONS",
        "TABLESPACES",
        "TABLESPACES_EXTENSIONS",
        
        // View dependency tables
        "VIEW_ROUTINE_USAGE",
        "VIEW_TABLE_USAGE",
        
        // Other system tables
        "COLUMN_STATISTICS",
        "KEYWORDS",
        "OPTIMIZER_TRACE",
        "PROFILING",
        "RESOURCE_GROUPS",
        "FILES",
        
        // InnoDB Buffer Pool tables
        "INNODB_BUFFER_PAGE",
        "INNODB_BUFFER_PAGE_LRU",
        "INNODB_BUFFER_POOL_STATS",
        "INNODB_CACHED_INDEXES",
        
        // InnoDB Compression tables
        "INNODB_CMP",
        "INNODB_CMP_RESET",
        "INNODB_CMP_PER_INDEX",
        "INNODB_CMP_PER_INDEX_RESET",
        "INNODB_CMPMEM",
        "INNODB_CMPMEM_RESET",
        
        // InnoDB Metadata tables
        "INNODB_COLUMNS",
        "INNODB_DATAFILES",
        "INNODB_FIELDS",
        "INNODB_FOREIGN",
        "INNODB_FOREIGN_COLS",
        "INNODB_INDEXES",
        "INNODB_TABLES",
        
        // InnoDB Full-text tables
        "INNODB_FT_BEING_DELETED",
        "INNODB_FT_CONFIG",
        "INNODB_FT_DEFAULT_STOPWORD",
        "INNODB_FT_DELETED",
        "INNODB_FT_INDEX_CACHE",
        "INNODB_FT_INDEX_TABLE",
        
        // InnoDB Tablespace and Transaction tables
        "INNODB_METRICS",
        "INNODB_SESSION_TEMP_TABLESPACES",
        "INNODB_TABLESPACES",
        "INNODB_TABLESPACES_BRIEF",
        "INNODB_TABLESTATS",
        "INNODB_TEMP_TABLE_INFO",
        "INNODB_TRX"
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
            // Core metadata tables
            "SCHEMATA" => GetSchemata(filterSchema),
            "TABLES" => GetTables(filterSchema, filterTable),
            "COLUMNS" => GetColumns(filterSchema, filterTable),
            "STATISTICS" => GetStatistics(filterSchema, filterTable),
            "ENGINES" => GetEngines(),
            "VIEWS" => GetViews(filterSchema, filterTable),
            
            // Character set and collation tables
            "CHARACTER_SETS" => GetCharacterSets(),
            "COLLATIONS" => GetCollations(),
            "COLLATION_CHARACTER_SET_APPLICABILITY" => GetCollationCharacterSetApplicability(),
            
            // Constraint tables
            "TABLE_CONSTRAINTS" => GetTableConstraints(filterSchema, filterTable),
            "CHECK_CONSTRAINTS" => GetCheckConstraints(filterSchema),
            "KEY_COLUMN_USAGE" => GetKeyColumnUsage(filterSchema, filterTable),
            "REFERENTIAL_CONSTRAINTS" => GetReferentialConstraints(filterSchema, filterTable),
            
            // System tables
            "PROCESSLIST" => GetProcesslist(),
            "PLUGINS" => GetPlugins(),
            "EVENTS" => GetEvents(filterSchema),
            "PARTITIONS" => GetPartitions(filterSchema, filterTable),
            
            // Routine tables
            "ROUTINES" => GetRoutines(filterSchema),
            "PARAMETERS" => GetParameters(filterSchema),
            "TRIGGERS" => GetTriggers(filterSchema, filterTable),
            
            // Privilege tables
            "COLUMN_PRIVILEGES" => GetColumnPrivileges(filterSchema, filterTable),
            "TABLE_PRIVILEGES" => GetTablePrivileges(filterSchema, filterTable),
            "SCHEMA_PRIVILEGES" => GetSchemaPrivileges(filterSchema),
            "USER_PRIVILEGES" => GetUserPrivileges(),
            "USER_ATTRIBUTES" => GetUserAttributes(),
            
            // Role tables
            "ADMINISTRABLE_ROLE_AUTHORIZATIONS" => GetAdministrableRoleAuthorizations(),
            "APPLICABLE_ROLES" => GetApplicableRoles(),
            "ENABLED_ROLES" => GetEnabledRoles(),
            "ROLE_COLUMN_GRANTS" => GetRoleColumnGrants(),
            "ROLE_ROUTINE_GRANTS" => GetRoleRoutineGrants(),
            "ROLE_TABLE_GRANTS" => GetRoleTableGrants(),
            
            // Extension tables
            "SCHEMATA_EXTENSIONS" => GetSchemataExtensions(filterSchema),
            "TABLES_EXTENSIONS" => GetTablesExtensions(filterSchema, filterTable),
            "COLUMNS_EXTENSIONS" => GetColumnsExtensions(filterSchema, filterTable),
            "TABLE_CONSTRAINTS_EXTENSIONS" => GetTableConstraintsExtensions(filterSchema),
            "TABLESPACES" => GetTablespaces(),
            "TABLESPACES_EXTENSIONS" => GetTablespacesExtensions(),
            
            // View dependency tables
            "VIEW_ROUTINE_USAGE" => GetViewRoutineUsage(filterSchema),
            "VIEW_TABLE_USAGE" => GetViewTableUsage(filterSchema),
            
            // Other system tables
            "COLUMN_STATISTICS" => GetColumnStatistics(filterSchema, filterTable),
            "KEYWORDS" => GetKeywords(),
            "OPTIMIZER_TRACE" => GetOptimizerTrace(),
            "PROFILING" => GetProfiling(),
            "RESOURCE_GROUPS" => GetResourceGroups(),
            "FILES" => GetFiles(),
            
            // InnoDB Buffer Pool tables
            "INNODB_BUFFER_PAGE" => GetInnodbBufferPage(),
            "INNODB_BUFFER_PAGE_LRU" => GetInnodbBufferPageLru(),
            "INNODB_BUFFER_POOL_STATS" => GetInnodbBufferPoolStats(),
            "INNODB_CACHED_INDEXES" => GetInnodbCachedIndexes(),
            
            // InnoDB Compression tables
            "INNODB_CMP" => GetInnodbCmp(),
            "INNODB_CMP_RESET" => GetInnodbCmpReset(),
            "INNODB_CMP_PER_INDEX" => GetInnodbCmpPerIndex(),
            "INNODB_CMP_PER_INDEX_RESET" => GetInnodbCmpPerIndexReset(),
            "INNODB_CMPMEM" => GetInnodbCmpmem(),
            "INNODB_CMPMEM_RESET" => GetInnodbCmpmemReset(),
            
            // InnoDB Metadata tables
            "INNODB_COLUMNS" => GetInnodbColumns(),
            "INNODB_DATAFILES" => GetInnodbDatafiles(),
            "INNODB_FIELDS" => GetInnodbFields(),
            "INNODB_FOREIGN" => GetInnodbForeign(),
            "INNODB_FOREIGN_COLS" => GetInnodbForeignCols(),
            "INNODB_INDEXES" => GetInnodbIndexes(),
            "INNODB_TABLES" => GetInnodbTables(),
            
            // InnoDB Full-text tables
            "INNODB_FT_BEING_DELETED" => GetInnodbFtBeingDeleted(),
            "INNODB_FT_CONFIG" => GetInnodbFtConfig(),
            "INNODB_FT_DEFAULT_STOPWORD" => GetInnodbFtDefaultStopword(),
            "INNODB_FT_DELETED" => GetInnodbFtDeleted(),
            "INNODB_FT_INDEX_CACHE" => GetInnodbFtIndexCache(),
            "INNODB_FT_INDEX_TABLE" => GetInnodbFtIndexTable(),
            
            // InnoDB Tablespace and Transaction tables
            "INNODB_METRICS" => GetInnodbMetrics(),
            "INNODB_SESSION_TEMP_TABLESPACES" => GetInnodbSessionTempTablespaces(),
            "INNODB_TABLESPACES" => GetInnodbTablespaces(),
            "INNODB_TABLESPACES_BRIEF" => GetInnodbTablespacesBrief(),
            "INNODB_TABLESTATS" => GetInnodbTablestats(),
            "INNODB_TEMP_TABLE_INFO" => GetInnodbTempTableInfo(),
            "INNODB_TRX" => GetInnodbTrx(),
            
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
            // Core metadata tables
            "SCHEMATA" => CreateSchemataSchema(),
            "TABLES" => CreateTablesSchema(),
            "COLUMNS" => CreateColumnsSchema(),
            "STATISTICS" => CreateStatisticsSchema(),
            "ENGINES" => CreateEnginesSchema(),
            "VIEWS" => CreateViewsSchema(),
            
            // Character set and collation tables
            "CHARACTER_SETS" => CreateCharacterSetsSchema(),
            "COLLATIONS" => CreateCollationsSchema(),
            "COLLATION_CHARACTER_SET_APPLICABILITY" => CreateCollationCharacterSetApplicabilitySchema(),
            
            // Constraint tables
            "TABLE_CONSTRAINTS" => CreateTableConstraintsSchema(),
            "CHECK_CONSTRAINTS" => CreateCheckConstraintsSchema(),
            "KEY_COLUMN_USAGE" => CreateKeyColumnUsageSchema(),
            "REFERENTIAL_CONSTRAINTS" => CreateReferentialConstraintsSchema(),
            
            // System tables
            "PROCESSLIST" => CreateProcesslistSchema(),
            "PLUGINS" => CreatePluginsSchema(),
            "EVENTS" => CreateEventsSchema(),
            "PARTITIONS" => CreatePartitionsSchema(),
            
            // Routine tables
            "ROUTINES" => CreateRoutinesSchema(),
            "PARAMETERS" => CreateParametersSchema(),
            "TRIGGERS" => CreateTriggersSchema(),
            
            // Privilege tables
            "COLUMN_PRIVILEGES" => CreateColumnPrivilegesSchema(),
            "TABLE_PRIVILEGES" => CreateTablePrivilegesSchema(),
            "SCHEMA_PRIVILEGES" => CreateSchemaPrivilegesSchema(),
            "USER_PRIVILEGES" => CreateUserPrivilegesSchema(),
            "USER_ATTRIBUTES" => CreateUserAttributesSchema(),
            
            // Role tables
            "ADMINISTRABLE_ROLE_AUTHORIZATIONS" => CreateAdministrableRoleAuthorizationsSchema(),
            "APPLICABLE_ROLES" => CreateApplicableRolesSchema(),
            "ENABLED_ROLES" => CreateEnabledRolesSchema(),
            "ROLE_COLUMN_GRANTS" => CreateRoleColumnGrantsSchema(),
            "ROLE_ROUTINE_GRANTS" => CreateRoleRoutineGrantsSchema(),
            "ROLE_TABLE_GRANTS" => CreateRoleTableGrantsSchema(),
            
            // Extension tables
            "SCHEMATA_EXTENSIONS" => CreateSchemataExtensionsSchema(),
            "TABLES_EXTENSIONS" => CreateTablesExtensionsSchema(),
            "COLUMNS_EXTENSIONS" => CreateColumnsExtensionsSchema(),
            "TABLE_CONSTRAINTS_EXTENSIONS" => CreateTableConstraintsExtensionsSchema(),
            "TABLESPACES" => CreateTablespacesSchema(),
            "TABLESPACES_EXTENSIONS" => CreateTablespacesExtensionsSchema(),
            
            // View dependency tables
            "VIEW_ROUTINE_USAGE" => CreateViewRoutineUsageSchema(),
            "VIEW_TABLE_USAGE" => CreateViewTableUsageSchema(),
            
            // Other system tables
            "COLUMN_STATISTICS" => CreateColumnStatisticsSchema(),
            "KEYWORDS" => CreateKeywordsSchema(),
            "OPTIMIZER_TRACE" => CreateOptimizerTraceSchema(),
            "PROFILING" => CreateProfilingSchema(),
            "RESOURCE_GROUPS" => CreateResourceGroupsSchema(),
            "FILES" => CreateFilesSchema(),
            
            // InnoDB Buffer Pool tables
            "INNODB_BUFFER_PAGE" => CreateInnodbBufferPageSchema(),
            "INNODB_BUFFER_PAGE_LRU" => CreateInnodbBufferPageLruSchema(),
            "INNODB_BUFFER_POOL_STATS" => CreateInnodbBufferPoolStatsSchema(),
            "INNODB_CACHED_INDEXES" => CreateInnodbCachedIndexesSchema(),
            
            // InnoDB Compression tables
            "INNODB_CMP" => CreateInnodbCmpSchema(),
            "INNODB_CMP_RESET" => CreateInnodbCmpResetSchema(),
            "INNODB_CMP_PER_INDEX" => CreateInnodbCmpPerIndexSchema(),
            "INNODB_CMP_PER_INDEX_RESET" => CreateInnodbCmpPerIndexResetSchema(),
            "INNODB_CMPMEM" => CreateInnodbCmpmemSchema(),
            "INNODB_CMPMEM_RESET" => CreateInnodbCmpmemResetSchema(),
            
            // InnoDB Metadata tables
            "INNODB_COLUMNS" => CreateInnodbColumnsSchema(),
            "INNODB_DATAFILES" => CreateInnodbDatafilesSchema(),
            "INNODB_FIELDS" => CreateInnodbFieldsSchema(),
            "INNODB_FOREIGN" => CreateInnodbForeignSchema(),
            "INNODB_FOREIGN_COLS" => CreateInnodbForeignColsSchema(),
            "INNODB_INDEXES" => CreateInnodbIndexesSchema(),
            "INNODB_TABLES" => CreateInnodbTablesSchema(),
            
            // InnoDB Full-text tables
            "INNODB_FT_BEING_DELETED" => CreateInnodbFtBeingDeletedSchema(),
            "INNODB_FT_CONFIG" => CreateInnodbFtConfigSchema(),
            "INNODB_FT_DEFAULT_STOPWORD" => CreateInnodbFtDefaultStopwordSchema(),
            "INNODB_FT_DELETED" => CreateInnodbFtDeletedSchema(),
            "INNODB_FT_INDEX_CACHE" => CreateInnodbFtIndexCacheSchema(),
            "INNODB_FT_INDEX_TABLE" => CreateInnodbFtIndexTableSchema(),
            
            // InnoDB Tablespace and Transaction tables
            "INNODB_METRICS" => CreateInnodbMetricsSchema(),
            "INNODB_SESSION_TEMP_TABLESPACES" => CreateInnodbSessionTempTablespacesSchema(),
            "INNODB_TABLESPACES" => CreateInnodbTablespacesSchema(),
            "INNODB_TABLESPACES_BRIEF" => CreateInnodbTablespacesBriefSchema(),
            "INNODB_TABLESTATS" => CreateInnodbTablestatsSchema(),
            "INNODB_TEMP_TABLE_INFO" => CreateInnodbTempTableInfoSchema(),
            "INNODB_TRX" => CreateInnodbTrxSchema(),
            
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
            new ColumnDefinition("VERSION", DataType.Int),
            new ColumnDefinition("ROW_FORMAT", DataType.VarChar, 10),
            new ColumnDefinition("TABLE_ROWS", DataType.BigInt),
            new ColumnDefinition("AVG_ROW_LENGTH", DataType.BigInt),
            new ColumnDefinition("DATA_LENGTH", DataType.BigInt),
            new ColumnDefinition("MAX_DATA_LENGTH", DataType.BigInt),
            new ColumnDefinition("INDEX_LENGTH", DataType.BigInt),
            new ColumnDefinition("DATA_FREE", DataType.BigInt),
            new ColumnDefinition("AUTO_INCREMENT", DataType.BigInt),
            new ColumnDefinition("CREATE_TIME", DataType.DateTime),
            new ColumnDefinition("UPDATE_TIME", DataType.DateTime),
            new ColumnDefinition("CHECK_TIME", DataType.DateTime),
            new ColumnDefinition("TABLE_COLLATION", DataType.VarChar, 64),
            new ColumnDefinition("CHECKSUM", DataType.BigInt),
            new ColumnDefinition("CREATE_OPTIONS", DataType.VarChar, 2048),
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

                // Get auto_increment value if any column has it
                long? autoIncrement = null;
                foreach (var col in table.Columns)
                {
                    if (col.IsAutoIncrement)
                    {
                        autoIncrement = table.GetNextAutoIncrementValue();
                        break;
                    }
                }

                result.Rows.Add([
                    DataValue.FromVarChar("def"),                              // TABLE_CATALOG
                    DataValue.FromVarChar(db.Name),                            // TABLE_SCHEMA
                    DataValue.FromVarChar(table.TableName),                    // TABLE_NAME
                    DataValue.FromVarChar("BASE TABLE"),                       // TABLE_TYPE
                    DataValue.FromVarChar("CyscaleDB"),                        // ENGINE
                    DataValue.FromInt(10),                                     // VERSION
                    DataValue.FromVarChar("Dynamic"),                          // ROW_FORMAT
                    DataValue.FromBigInt(table.RowCount),                      // TABLE_ROWS
                    DataValue.FromBigInt(0),                                   // AVG_ROW_LENGTH
                    DataValue.FromBigInt(0),                                   // DATA_LENGTH
                    DataValue.FromBigInt(0),                                   // MAX_DATA_LENGTH
                    DataValue.FromBigInt(0),                                   // INDEX_LENGTH
                    DataValue.FromBigInt(0),                                   // DATA_FREE
                    autoIncrement.HasValue ? DataValue.FromBigInt(autoIncrement.Value) : DataValue.Null, // AUTO_INCREMENT
                    DataValue.FromDateTime(table.CreatedAt),                   // CREATE_TIME
                    DataValue.Null,                                            // UPDATE_TIME
                    DataValue.Null,                                            // CHECK_TIME
                    DataValue.FromVarChar("utf8mb4_general_ci"),               // TABLE_COLLATION
                    DataValue.Null,                                            // CHECKSUM
                    DataValue.FromVarChar(""),                                 // CREATE_OPTIONS
                    DataValue.FromVarChar("")                                  // TABLE_COMMENT
                ]);
            }

            // Include views
            foreach (var view in db.Views)
            {
                if (filterTable != null && !view.ViewName.Equals(filterTable, StringComparison.OrdinalIgnoreCase))
                    continue;

                result.Rows.Add([
                    DataValue.FromVarChar("def"),                              // TABLE_CATALOG
                    DataValue.FromVarChar(db.Name),                            // TABLE_SCHEMA
                    DataValue.FromVarChar(view.ViewName),                      // TABLE_NAME
                    DataValue.FromVarChar("VIEW"),                             // TABLE_TYPE
                    DataValue.Null,                                            // ENGINE
                    DataValue.Null,                                            // VERSION
                    DataValue.Null,                                            // ROW_FORMAT
                    DataValue.Null,                                            // TABLE_ROWS
                    DataValue.Null,                                            // AVG_ROW_LENGTH
                    DataValue.Null,                                            // DATA_LENGTH
                    DataValue.Null,                                            // MAX_DATA_LENGTH
                    DataValue.Null,                                            // INDEX_LENGTH
                    DataValue.Null,                                            // DATA_FREE
                    DataValue.Null,                                            // AUTO_INCREMENT
                    DataValue.Null,                                            // CREATE_TIME
                    DataValue.Null,                                            // UPDATE_TIME
                    DataValue.Null,                                            // CHECK_TIME
                    DataValue.Null,                                            // TABLE_COLLATION
                    DataValue.Null,                                            // CHECKSUM
                    DataValue.Null,                                            // CREATE_OPTIONS
                    DataValue.FromVarChar("VIEW")                              // TABLE_COMMENT
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
            new ColumnDefinition("CHARACTER_MAXIMUM_LENGTH", DataType.BigInt),
            new ColumnDefinition("CHARACTER_OCTET_LENGTH", DataType.BigInt),
            new ColumnDefinition("NUMERIC_PRECISION", DataType.BigInt),
            new ColumnDefinition("NUMERIC_SCALE", DataType.BigInt),
            new ColumnDefinition("DATETIME_PRECISION", DataType.Int),
            new ColumnDefinition("CHARACTER_SET_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLLATION_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLUMN_TYPE", DataType.Text),
            new ColumnDefinition("COLUMN_KEY", DataType.VarChar, 3),
            new ColumnDefinition("EXTRA", DataType.VarChar, 256),
            new ColumnDefinition("PRIVILEGES", DataType.VarChar, 154),
            new ColumnDefinition("COLUMN_COMMENT", DataType.Text),
            new ColumnDefinition("GENERATION_EXPRESSION", DataType.Text),
            new ColumnDefinition("SRS_ID", DataType.Int)
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
                    // Calculate character/numeric properties based on data type
                    long? charMaxLen = null;
                    long? charOctetLen = null;
                    long? numericPrecision = null;
                    long? numericScale = null;
                    int? datetimePrecision = null;
                    string? charsetName = null;
                    string? collationName = null;

                    switch (col.DataType)
                    {
                        case DataType.VarChar:
                        case DataType.Char:
                        case DataType.Text:
                            charMaxLen = col.MaxLength > 0 ? col.MaxLength : 65535;
                            charOctetLen = charMaxLen * 4; // UTF-8 max 4 bytes per char
                            charsetName = "utf8mb4";
                            collationName = "utf8mb4_general_ci";
                            break;
                        case DataType.TinyInt:
                            numericPrecision = 3;
                            numericScale = 0;
                            break;
                        case DataType.SmallInt:
                            numericPrecision = 5;
                            numericScale = 0;
                            break;
                        case DataType.Int:
                            numericPrecision = 10;
                            numericScale = 0;
                            break;
                        case DataType.BigInt:
                            numericPrecision = 19;
                            numericScale = 0;
                            break;
                        case DataType.Float:
                            numericPrecision = 12;
                            break;
                        case DataType.Double:
                            numericPrecision = 22;
                            break;
                        case DataType.Decimal:
                            numericPrecision = col.Precision > 0 ? col.Precision : 10;
                            numericScale = col.Scale;
                            break;
                        case DataType.DateTime:
                        case DataType.Timestamp:
                            datetimePrecision = 0;
                            break;
                        case DataType.Time:
                            datetimePrecision = 0;
                            break;
                    }

                    result.Rows.Add([
                        DataValue.FromVarChar("def"),                                                          // TABLE_CATALOG
                        DataValue.FromVarChar(db.Name),                                                        // TABLE_SCHEMA
                        DataValue.FromVarChar(table.TableName),                                                // TABLE_NAME
                        DataValue.FromVarChar(col.Name),                                                       // COLUMN_NAME
                        DataValue.FromInt(ordinal++),                                                          // ORDINAL_POSITION
                        col.DefaultValue.HasValue ? DataValue.FromVarChar(col.DefaultValue.Value.ToString()) : DataValue.Null, // COLUMN_DEFAULT
                        DataValue.FromVarChar(col.IsNullable ? "YES" : "NO"),                                  // IS_NULLABLE
                        DataValue.FromVarChar(GetMySqlDataTypeName(col.DataType)),                             // DATA_TYPE
                        charMaxLen.HasValue ? DataValue.FromBigInt(charMaxLen.Value) : DataValue.Null,         // CHARACTER_MAXIMUM_LENGTH
                        charOctetLen.HasValue ? DataValue.FromBigInt(charOctetLen.Value) : DataValue.Null,     // CHARACTER_OCTET_LENGTH
                        numericPrecision.HasValue ? DataValue.FromBigInt(numericPrecision.Value) : DataValue.Null, // NUMERIC_PRECISION
                        numericScale.HasValue ? DataValue.FromBigInt(numericScale.Value) : DataValue.Null,     // NUMERIC_SCALE
                        datetimePrecision.HasValue ? DataValue.FromInt(datetimePrecision.Value) : DataValue.Null, // DATETIME_PRECISION
                        charsetName != null ? DataValue.FromVarChar(charsetName) : DataValue.Null,             // CHARACTER_SET_NAME
                        collationName != null ? DataValue.FromVarChar(collationName) : DataValue.Null,         // COLLATION_NAME
                        DataValue.FromVarChar(GetColumnType(col)),                                             // COLUMN_TYPE
                        DataValue.FromVarChar(col.IsPrimaryKey ? "PRI" : ""),                                  // COLUMN_KEY
                        DataValue.FromVarChar(col.IsAutoIncrement ? "auto_increment" : ""),                    // EXTRA
                        DataValue.FromVarChar("select,insert,update,references"),                              // PRIVILEGES
                        DataValue.FromVarChar(""),                                                             // COLUMN_COMMENT
                        DataValue.Null,                                                                        // GENERATION_EXPRESSION
                        DataValue.Null                                                                         // SRS_ID
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

    #region PARAMETERS (Stored procedure parameters - stub)

    private static TableSchema CreateParametersSchema()
    {
        return new TableSchema(0, DatabaseName, "PARAMETERS",
        [
            new ColumnDefinition("SPECIFIC_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("SPECIFIC_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("SPECIFIC_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ORDINAL_POSITION", DataType.Int),
            new ColumnDefinition("PARAMETER_MODE", DataType.VarChar, 5),
            new ColumnDefinition("PARAMETER_NAME", DataType.VarChar, 64),
            new ColumnDefinition("DATA_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("CHARACTER_MAXIMUM_LENGTH", DataType.BigInt),
            new ColumnDefinition("CHARACTER_OCTET_LENGTH", DataType.BigInt),
            new ColumnDefinition("NUMERIC_PRECISION", DataType.Int),
            new ColumnDefinition("NUMERIC_SCALE", DataType.Int),
            new ColumnDefinition("DATETIME_PRECISION", DataType.Int),
            new ColumnDefinition("CHARACTER_SET_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLLATION_NAME", DataType.VarChar, 64),
            new ColumnDefinition("DTD_IDENTIFIER", DataType.Text),
            new ColumnDefinition("ROUTINE_TYPE", DataType.VarChar, 9)
        ]);
    }

    private ResultSet GetParameters(string? filterSchema)
    {
        // Return empty result - CyscaleDB doesn't support stored procedures yet
        return ResultSet.FromSchema(CreateParametersSchema());
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

    #region VIEWS (View definitions)

    private static TableSchema CreateViewsSchema()
    {
        return new TableSchema(0, DatabaseName, "VIEWS",
        [
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("VIEW_DEFINITION", DataType.Text),
            new ColumnDefinition("CHECK_OPTION", DataType.VarChar, 8),
            new ColumnDefinition("IS_UPDATABLE", DataType.VarChar, 3),
            new ColumnDefinition("DEFINER", DataType.VarChar, 288),
            new ColumnDefinition("SECURITY_TYPE", DataType.VarChar, 7),
            new ColumnDefinition("CHARACTER_SET_CLIENT", DataType.VarChar, 32),
            new ColumnDefinition("COLLATION_CONNECTION", DataType.VarChar, 32)
        ]);
    }

    private ResultSet GetViews(string? filterSchema, string? filterTable)
    {
        var result = ResultSet.FromSchema(CreateViewsSchema());

        foreach (var db in _catalog.Databases)
        {
            if (filterSchema != null && !db.Name.Equals(filterSchema, StringComparison.OrdinalIgnoreCase))
                continue;

            foreach (var view in db.Views)
            {
                if (filterTable != null && !view.ViewName.Equals(filterTable, StringComparison.OrdinalIgnoreCase))
                    continue;

                result.Rows.Add([
                    DataValue.FromVarChar("def"),
                    DataValue.FromVarChar(db.Name),
                    DataValue.FromVarChar(view.ViewName),
                    DataValue.FromVarChar(view.Definition),
                    DataValue.FromVarChar("NONE"),
                    DataValue.FromVarChar("NO"),
                    DataValue.FromVarChar("root@localhost"),
                    DataValue.FromVarChar("DEFINER"),
                    DataValue.FromVarChar("utf8mb4"),
                    DataValue.FromVarChar("utf8mb4_general_ci")
                ]);
            }
        }

        return result;
    }

    #endregion

    #region TRIGGERS (Trigger definitions - stub)

    private static TableSchema CreateTriggersSchema()
    {
        return new TableSchema(0, DatabaseName, "TRIGGERS",
        [
            new ColumnDefinition("TRIGGER_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TRIGGER_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TRIGGER_NAME", DataType.VarChar, 64),
            new ColumnDefinition("EVENT_MANIPULATION", DataType.VarChar, 6),
            new ColumnDefinition("EVENT_OBJECT_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("EVENT_OBJECT_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("EVENT_OBJECT_TABLE", DataType.VarChar, 64),
            new ColumnDefinition("ACTION_ORDER", DataType.Int),
            new ColumnDefinition("ACTION_CONDITION", DataType.Text),
            new ColumnDefinition("ACTION_STATEMENT", DataType.Text),
            new ColumnDefinition("ACTION_ORIENTATION", DataType.VarChar, 9),
            new ColumnDefinition("ACTION_TIMING", DataType.VarChar, 6),
            new ColumnDefinition("ACTION_REFERENCE_OLD_TABLE", DataType.VarChar, 64),
            new ColumnDefinition("ACTION_REFERENCE_NEW_TABLE", DataType.VarChar, 64),
            new ColumnDefinition("ACTION_REFERENCE_OLD_ROW", DataType.VarChar, 3),
            new ColumnDefinition("ACTION_REFERENCE_NEW_ROW", DataType.VarChar, 3),
            new ColumnDefinition("CREATED", DataType.DateTime),
            new ColumnDefinition("SQL_MODE", DataType.VarChar, 8192),
            new ColumnDefinition("DEFINER", DataType.VarChar, 288),
            new ColumnDefinition("CHARACTER_SET_CLIENT", DataType.VarChar, 32),
            new ColumnDefinition("COLLATION_CONNECTION", DataType.VarChar, 32),
            new ColumnDefinition("DATABASE_COLLATION", DataType.VarChar, 32)
        ]);
    }

    private ResultSet GetTriggers(string? filterSchema, string? filterTable)
    {
        // Return empty result - CyscaleDB doesn't support triggers yet
        return ResultSet.FromSchema(CreateTriggersSchema());
    }

    #endregion

    #region CHARACTER_SETS

    private static TableSchema CreateCharacterSetsSchema()
    {
        return new TableSchema(0, DatabaseName, "CHARACTER_SETS",
        [
            new ColumnDefinition("CHARACTER_SET_NAME", DataType.VarChar, 64),
            new ColumnDefinition("DEFAULT_COLLATE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("DESCRIPTION", DataType.VarChar, 2048),
            new ColumnDefinition("MAXLEN", DataType.Int)
        ]);
    }

    private ResultSet GetCharacterSets()
    {
        var result = ResultSet.FromSchema(CreateCharacterSetsSchema());

        // Common MySQL character sets
        var charsets = new[]
        {
            ("armscii8", "armscii8_general_ci", "ARMSCII-8 Armenian", 1),
            ("ascii", "ascii_general_ci", "US ASCII", 1),
            ("big5", "big5_chinese_ci", "Big5 Traditional Chinese", 2),
            ("binary", "binary", "Binary pseudo charset", 1),
            ("cp1250", "cp1250_general_ci", "Windows Central European", 1),
            ("cp1251", "cp1251_general_ci", "Windows Cyrillic", 1),
            ("cp1256", "cp1256_general_ci", "Windows Arabic", 1),
            ("cp1257", "cp1257_general_ci", "Windows Baltic", 1),
            ("cp850", "cp850_general_ci", "DOS West European", 1),
            ("cp852", "cp852_general_ci", "DOS Central European", 1),
            ("cp866", "cp866_general_ci", "DOS Russian", 1),
            ("cp932", "cp932_japanese_ci", "SJIS for Windows Japanese", 2),
            ("dec8", "dec8_swedish_ci", "DEC West European", 1),
            ("eucjpms", "eucjpms_japanese_ci", "UJIS for Windows Japanese", 3),
            ("euckr", "euckr_korean_ci", "EUC-KR Korean", 2),
            ("gb18030", "gb18030_chinese_ci", "China National Standard GB18030", 4),
            ("gb2312", "gb2312_chinese_ci", "GB2312 Simplified Chinese", 2),
            ("gbk", "gbk_chinese_ci", "GBK Simplified Chinese", 2),
            ("geostd8", "geostd8_general_ci", "GEOSTD8 Georgian", 1),
            ("greek", "greek_general_ci", "ISO 8859-7 Greek", 1),
            ("hebrew", "hebrew_general_ci", "ISO 8859-8 Hebrew", 1),
            ("hp8", "hp8_english_ci", "HP West European", 1),
            ("keybcs2", "keybcs2_general_ci", "DOS Kamenicky Czech-Slovak", 1),
            ("koi8r", "koi8r_general_ci", "KOI8-R Relcom Russian", 1),
            ("koi8u", "koi8u_general_ci", "KOI8-U Ukrainian", 1),
            ("latin1", "latin1_swedish_ci", "cp1252 West European", 1),
            ("latin2", "latin2_general_ci", "ISO 8859-2 Central European", 1),
            ("latin5", "latin5_turkish_ci", "ISO 8859-9 Turkish", 1),
            ("latin7", "latin7_general_ci", "ISO 8859-13 Baltic", 1),
            ("macce", "macce_general_ci", "Mac Central European", 1),
            ("macroman", "macroman_general_ci", "Mac West European", 1),
            ("sjis", "sjis_japanese_ci", "Shift-JIS Japanese", 2),
            ("swe7", "swe7_swedish_ci", "7bit Swedish", 1),
            ("tis620", "tis620_thai_ci", "TIS620 Thai", 1),
            ("ucs2", "ucs2_general_ci", "UCS-2 Unicode", 2),
            ("ujis", "ujis_japanese_ci", "EUC-JP Japanese", 3),
            ("utf16", "utf16_general_ci", "UTF-16 Unicode", 4),
            ("utf16le", "utf16le_general_ci", "UTF-16LE Unicode", 4),
            ("utf32", "utf32_general_ci", "UTF-32 Unicode", 4),
            ("utf8", "utf8_general_ci", "UTF-8 Unicode", 3),
            ("utf8mb3", "utf8mb3_general_ci", "UTF-8 Unicode", 3),
            ("utf8mb4", "utf8mb4_general_ci", "UTF-8 Unicode", 4)
        };

        foreach (var (name, collation, description, maxlen) in charsets)
        {
            result.Rows.Add([
                DataValue.FromVarChar(name),
                DataValue.FromVarChar(collation),
                DataValue.FromVarChar(description),
                DataValue.FromInt(maxlen)
            ]);
        }

        return result;
    }

    #endregion

    #region COLLATIONS

    private static TableSchema CreateCollationsSchema()
    {
        return new TableSchema(0, DatabaseName, "COLLATIONS",
        [
            new ColumnDefinition("COLLATION_NAME", DataType.VarChar, 64),
            new ColumnDefinition("CHARACTER_SET_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ID", DataType.BigInt),
            new ColumnDefinition("IS_DEFAULT", DataType.VarChar, 3),
            new ColumnDefinition("IS_COMPILED", DataType.VarChar, 3),
            new ColumnDefinition("SORTLEN", DataType.Int),
            new ColumnDefinition("PAD_ATTRIBUTE", DataType.VarChar, 9)
        ]);
    }

    private ResultSet GetCollations()
    {
        var result = ResultSet.FromSchema(CreateCollationsSchema());

        // Common MySQL collations
        var collations = new[]
        {
            ("utf8mb4_general_ci", "utf8mb4", 45L, "Yes", 1),
            ("utf8mb4_unicode_ci", "utf8mb4", 224L, "", 8),
            ("utf8mb4_bin", "utf8mb4", 46L, "", 1),
            ("utf8mb4_0900_ai_ci", "utf8mb4", 255L, "", 0),
            ("utf8_general_ci", "utf8", 33L, "Yes", 1),
            ("utf8_unicode_ci", "utf8", 192L, "", 8),
            ("utf8_bin", "utf8", 83L, "", 1),
            ("latin1_swedish_ci", "latin1", 8L, "Yes", 1),
            ("latin1_general_ci", "latin1", 48L, "", 1),
            ("latin1_bin", "latin1", 47L, "", 1),
            ("ascii_general_ci", "ascii", 11L, "Yes", 1),
            ("ascii_bin", "ascii", 65L, "", 1),
            ("binary", "binary", 63L, "Yes", 1),
            ("gbk_chinese_ci", "gbk", 28L, "Yes", 1),
            ("gbk_bin", "gbk", 87L, "", 1),
            ("big5_chinese_ci", "big5", 1L, "Yes", 1),
            ("big5_bin", "big5", 84L, "", 1)
        };

        foreach (var (collation, charset, id, isDefault, sortlen) in collations)
        {
            result.Rows.Add([
                DataValue.FromVarChar(collation),
                DataValue.FromVarChar(charset),
                DataValue.FromBigInt(id),
                DataValue.FromVarChar(string.IsNullOrEmpty(isDefault) ? "" : "Yes"),
                DataValue.FromVarChar("Yes"),
                DataValue.FromInt(sortlen),
                DataValue.FromVarChar("PAD SPACE")
            ]);
        }

        return result;
    }

    #endregion

    #region COLLATION_CHARACTER_SET_APPLICABILITY

    private static TableSchema CreateCollationCharacterSetApplicabilitySchema()
    {
        return new TableSchema(0, DatabaseName, "COLLATION_CHARACTER_SET_APPLICABILITY",
        [
            new ColumnDefinition("COLLATION_NAME", DataType.VarChar, 64),
            new ColumnDefinition("CHARACTER_SET_NAME", DataType.VarChar, 64)
        ]);
    }

    private ResultSet GetCollationCharacterSetApplicability()
    {
        var result = ResultSet.FromSchema(CreateCollationCharacterSetApplicabilitySchema());

        // Map collations to their character sets
        var mappings = new[]
        {
            ("utf8mb4_general_ci", "utf8mb4"),
            ("utf8mb4_unicode_ci", "utf8mb4"),
            ("utf8mb4_bin", "utf8mb4"),
            ("utf8mb4_0900_ai_ci", "utf8mb4"),
            ("utf8_general_ci", "utf8"),
            ("utf8_unicode_ci", "utf8"),
            ("utf8_bin", "utf8"),
            ("latin1_swedish_ci", "latin1"),
            ("latin1_general_ci", "latin1"),
            ("latin1_bin", "latin1"),
            ("ascii_general_ci", "ascii"),
            ("ascii_bin", "ascii"),
            ("binary", "binary"),
            ("gbk_chinese_ci", "gbk"),
            ("gbk_bin", "gbk"),
            ("big5_chinese_ci", "big5"),
            ("big5_bin", "big5")
        };

        foreach (var (collation, charset) in mappings)
        {
            result.Rows.Add([
                DataValue.FromVarChar(collation),
                DataValue.FromVarChar(charset)
            ]);
        }

        return result;
    }

    #endregion

    #region TABLE_CONSTRAINTS

    private static TableSchema CreateTableConstraintsSchema()
    {
        return new TableSchema(0, DatabaseName, "TABLE_CONSTRAINTS",
        [
            new ColumnDefinition("CONSTRAINT_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_NAME", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_TYPE", DataType.VarChar, 11),
            new ColumnDefinition("ENFORCED", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetTableConstraints(string? filterSchema, string? filterTable)
    {
        var result = ResultSet.FromSchema(CreateTableConstraintsSchema());

        foreach (var db in _catalog.Databases)
        {
            if (filterSchema != null && !db.Name.Equals(filterSchema, StringComparison.OrdinalIgnoreCase))
                continue;

            foreach (var table in db.Tables)
            {
                if (filterTable != null && !table.TableName.Equals(filterTable, StringComparison.OrdinalIgnoreCase))
                    continue;

                // Check for PRIMARY KEY
                bool hasPrimaryKey = false;
                foreach (var col in table.Columns)
                {
                    if (col.IsPrimaryKey)
                    {
                        hasPrimaryKey = true;
                        break;
                    }
                }

                if (hasPrimaryKey)
                {
                    result.Rows.Add([
                        DataValue.FromVarChar("def"),
                        DataValue.FromVarChar(db.Name),
                        DataValue.FromVarChar("PRIMARY"),
                        DataValue.FromVarChar(db.Name),
                        DataValue.FromVarChar(table.TableName),
                        DataValue.FromVarChar("PRIMARY KEY"),
                        DataValue.FromVarChar("YES")
                    ]);
                }

                // Note: UNIQUE constraints would require additional index tracking
                // Currently not implemented as ColumnDefinition doesn't have IsUnique property
            }
        }

        return result;
    }

    #endregion

    #region CHECK_CONSTRAINTS

    private static TableSchema CreateCheckConstraintsSchema()
    {
        return new TableSchema(0, DatabaseName, "CHECK_CONSTRAINTS",
        [
            new ColumnDefinition("CONSTRAINT_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_NAME", DataType.VarChar, 64),
            new ColumnDefinition("CHECK_CLAUSE", DataType.Text)
        ]);
    }

    private ResultSet GetCheckConstraints(string? filterSchema)
    {
        // Return empty result - CyscaleDB doesn't track CHECK constraints yet
        return ResultSet.FromSchema(CreateCheckConstraintsSchema());
    }

    #endregion

    #region PROCESSLIST

    private static TableSchema CreateProcesslistSchema()
    {
        return new TableSchema(0, DatabaseName, "PROCESSLIST",
        [
            new ColumnDefinition("ID", DataType.BigInt),
            new ColumnDefinition("USER", DataType.VarChar, 32),
            new ColumnDefinition("HOST", DataType.VarChar, 261),
            new ColumnDefinition("DB", DataType.VarChar, 64),
            new ColumnDefinition("COMMAND", DataType.VarChar, 16),
            new ColumnDefinition("TIME", DataType.Int),
            new ColumnDefinition("STATE", DataType.VarChar, 64),
            new ColumnDefinition("INFO", DataType.Text),
            new ColumnDefinition("TIME_MS", DataType.BigInt),
            new ColumnDefinition("ROWS_SENT", DataType.BigInt),
            new ColumnDefinition("ROWS_EXAMINED", DataType.BigInt)
        ]);
    }

    private ResultSet GetProcesslist()
    {
        var result = ResultSet.FromSchema(CreateProcesslistSchema());

        // Return current connection as a single process
        result.Rows.Add([
            DataValue.FromBigInt(1),
            DataValue.FromVarChar("root"),
            DataValue.FromVarChar("localhost"),
            DataValue.Null,
            DataValue.FromVarChar("Query"),
            DataValue.FromInt(0),
            DataValue.FromVarChar("executing"),
            DataValue.FromVarChar("SELECT * FROM information_schema.PROCESSLIST"),
            DataValue.FromBigInt(0),
            DataValue.FromBigInt(0),
            DataValue.FromBigInt(0)
        ]);

        return result;
    }

    #endregion

    #region PLUGINS

    private static TableSchema CreatePluginsSchema()
    {
        return new TableSchema(0, DatabaseName, "PLUGINS",
        [
            new ColumnDefinition("PLUGIN_NAME", DataType.VarChar, 64),
            new ColumnDefinition("PLUGIN_VERSION", DataType.VarChar, 20),
            new ColumnDefinition("PLUGIN_STATUS", DataType.VarChar, 16),
            new ColumnDefinition("PLUGIN_TYPE", DataType.VarChar, 80),
            new ColumnDefinition("PLUGIN_TYPE_VERSION", DataType.VarChar, 20),
            new ColumnDefinition("PLUGIN_LIBRARY", DataType.VarChar, 64),
            new ColumnDefinition("PLUGIN_LIBRARY_VERSION", DataType.VarChar, 20),
            new ColumnDefinition("PLUGIN_AUTHOR", DataType.VarChar, 64),
            new ColumnDefinition("PLUGIN_DESCRIPTION", DataType.Text),
            new ColumnDefinition("PLUGIN_LICENSE", DataType.VarChar, 80),
            new ColumnDefinition("LOAD_OPTION", DataType.VarChar, 64)
        ]);
    }

    private ResultSet GetPlugins()
    {
        var result = ResultSet.FromSchema(CreatePluginsSchema());

        // Return CyscaleDB as a storage engine plugin
        result.Rows.Add([
            DataValue.FromVarChar("CyscaleDB"),
            DataValue.FromVarChar("1.0"),
            DataValue.FromVarChar("ACTIVE"),
            DataValue.FromVarChar("STORAGE ENGINE"),
            DataValue.FromVarChar("100"),
            DataValue.Null,
            DataValue.Null,
            DataValue.FromVarChar("CyscaleDB Team"),
            DataValue.FromVarChar("CyscaleDB native storage engine with MVCC support"),
            DataValue.FromVarChar("MIT"),
            DataValue.FromVarChar("ON")
        ]);

        return result;
    }

    #endregion

    #region EVENTS

    private static TableSchema CreateEventsSchema()
    {
        return new TableSchema(0, DatabaseName, "EVENTS",
        [
            new ColumnDefinition("EVENT_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("EVENT_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("EVENT_NAME", DataType.VarChar, 64),
            new ColumnDefinition("DEFINER", DataType.VarChar, 288),
            new ColumnDefinition("TIME_ZONE", DataType.VarChar, 64),
            new ColumnDefinition("EVENT_BODY", DataType.VarChar, 8),
            new ColumnDefinition("EVENT_DEFINITION", DataType.Text),
            new ColumnDefinition("EVENT_TYPE", DataType.VarChar, 9),
            new ColumnDefinition("EXECUTE_AT", DataType.DateTime),
            new ColumnDefinition("INTERVAL_VALUE", DataType.VarChar, 256),
            new ColumnDefinition("INTERVAL_FIELD", DataType.VarChar, 18),
            new ColumnDefinition("SQL_MODE", DataType.VarChar, 8192),
            new ColumnDefinition("STARTS", DataType.DateTime),
            new ColumnDefinition("ENDS", DataType.DateTime),
            new ColumnDefinition("STATUS", DataType.VarChar, 18),
            new ColumnDefinition("ON_COMPLETION", DataType.VarChar, 12),
            new ColumnDefinition("CREATED", DataType.DateTime),
            new ColumnDefinition("LAST_ALTERED", DataType.DateTime),
            new ColumnDefinition("LAST_EXECUTED", DataType.DateTime),
            new ColumnDefinition("EVENT_COMMENT", DataType.VarChar, 2048),
            new ColumnDefinition("ORIGINATOR", DataType.Int),
            new ColumnDefinition("CHARACTER_SET_CLIENT", DataType.VarChar, 32),
            new ColumnDefinition("COLLATION_CONNECTION", DataType.VarChar, 32),
            new ColumnDefinition("DATABASE_COLLATION", DataType.VarChar, 32)
        ]);
    }

    private ResultSet GetEvents(string? filterSchema)
    {
        // Return empty result - CyscaleDB doesn't support events yet
        return ResultSet.FromSchema(CreateEventsSchema());
    }

    #endregion

    #region PARTITIONS

    private static TableSchema CreatePartitionsSchema()
    {
        return new TableSchema(0, DatabaseName, "PARTITIONS",
        [
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("PARTITION_NAME", DataType.VarChar, 64),
            new ColumnDefinition("SUBPARTITION_NAME", DataType.VarChar, 64),
            new ColumnDefinition("PARTITION_ORDINAL_POSITION", DataType.Int),
            new ColumnDefinition("SUBPARTITION_ORDINAL_POSITION", DataType.Int),
            new ColumnDefinition("PARTITION_METHOD", DataType.VarChar, 18),
            new ColumnDefinition("SUBPARTITION_METHOD", DataType.VarChar, 12),
            new ColumnDefinition("PARTITION_EXPRESSION", DataType.Text),
            new ColumnDefinition("SUBPARTITION_EXPRESSION", DataType.Text),
            new ColumnDefinition("PARTITION_DESCRIPTION", DataType.Text),
            new ColumnDefinition("TABLE_ROWS", DataType.BigInt),
            new ColumnDefinition("AVG_ROW_LENGTH", DataType.BigInt),
            new ColumnDefinition("DATA_LENGTH", DataType.BigInt),
            new ColumnDefinition("MAX_DATA_LENGTH", DataType.BigInt),
            new ColumnDefinition("INDEX_LENGTH", DataType.BigInt),
            new ColumnDefinition("DATA_FREE", DataType.BigInt),
            new ColumnDefinition("CREATE_TIME", DataType.DateTime),
            new ColumnDefinition("UPDATE_TIME", DataType.DateTime),
            new ColumnDefinition("CHECK_TIME", DataType.DateTime),
            new ColumnDefinition("CHECKSUM", DataType.BigInt),
            new ColumnDefinition("PARTITION_COMMENT", DataType.Text),
            new ColumnDefinition("NODEGROUP", DataType.VarChar, 256),
            new ColumnDefinition("TABLESPACE_NAME", DataType.VarChar, 268)
        ]);
    }

    private ResultSet GetPartitions(string? filterSchema, string? filterTable)
    {
        // Return empty result - CyscaleDB doesn't support partitions yet
        return ResultSet.FromSchema(CreatePartitionsSchema());
    }

    #endregion

    #region COLUMN_PRIVILEGES

    private static TableSchema CreateColumnPrivilegesSchema()
    {
        return new TableSchema(0, DatabaseName, "COLUMN_PRIVILEGES",
        [
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLUMN_NAME", DataType.VarChar, 64),
            new ColumnDefinition("PRIVILEGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetColumnPrivileges(string? filterSchema, string? filterTable)
    {
        // Return empty result - CyscaleDB doesn't track column privileges
        return ResultSet.FromSchema(CreateColumnPrivilegesSchema());
    }

    #endregion

    #region TABLE_PRIVILEGES

    private static TableSchema CreateTablePrivilegesSchema()
    {
        return new TableSchema(0, DatabaseName, "TABLE_PRIVILEGES",
        [
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("PRIVILEGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetTablePrivileges(string? filterSchema, string? filterTable)
    {
        // Return empty result - CyscaleDB doesn't track table privileges
        return ResultSet.FromSchema(CreateTablePrivilegesSchema());
    }

    #endregion

    #region SCHEMA_PRIVILEGES

    private static TableSchema CreateSchemaPrivilegesSchema()
    {
        return new TableSchema(0, DatabaseName, "SCHEMA_PRIVILEGES",
        [
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("PRIVILEGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetSchemaPrivileges(string? filterSchema)
    {
        // Return empty result - CyscaleDB doesn't track schema privileges
        return ResultSet.FromSchema(CreateSchemaPrivilegesSchema());
    }

    #endregion

    #region USER_PRIVILEGES

    private static TableSchema CreateUserPrivilegesSchema()
    {
        return new TableSchema(0, DatabaseName, "USER_PRIVILEGES",
        [
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("PRIVILEGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetUserPrivileges()
    {
        var result = ResultSet.FromSchema(CreateUserPrivilegesSchema());

        // Return root user with all privileges
        var privileges = new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "RELOAD", 
                                  "SHUTDOWN", "PROCESS", "FILE", "REFERENCES", "INDEX", "ALTER", 
                                  "SHOW DATABASES", "SUPER", "CREATE TEMPORARY TABLES", "LOCK TABLES",
                                  "EXECUTE", "REPLICATION SLAVE", "REPLICATION CLIENT", "CREATE VIEW",
                                  "SHOW VIEW", "CREATE ROUTINE", "ALTER ROUTINE", "CREATE USER", "EVENT",
                                  "TRIGGER", "CREATE TABLESPACE", "CREATE ROLE", "DROP ROLE" };

        foreach (var priv in privileges)
        {
            result.Rows.Add([
                DataValue.FromVarChar("'root'@'%'"),
                DataValue.FromVarChar("def"),
                DataValue.FromVarChar(priv),
                DataValue.FromVarChar("YES")
            ]);
        }

        return result;
    }

    #endregion

    #region USER_ATTRIBUTES

    private static TableSchema CreateUserAttributesSchema()
    {
        return new TableSchema(0, DatabaseName, "USER_ATTRIBUTES",
        [
            new ColumnDefinition("USER", DataType.VarChar, 32),
            new ColumnDefinition("HOST", DataType.VarChar, 255),
            new ColumnDefinition("ATTRIBUTE", DataType.Text)
        ]);
    }

    private ResultSet GetUserAttributes()
    {
        var result = ResultSet.FromSchema(CreateUserAttributesSchema());

        // Return root user
        result.Rows.Add([
            DataValue.FromVarChar("root"),
            DataValue.FromVarChar("%"),
            DataValue.Null
        ]);

        return result;
    }

    #endregion

    #region ADMINISTRABLE_ROLE_AUTHORIZATIONS

    private static TableSchema CreateAdministrableRoleAuthorizationsSchema()
    {
        return new TableSchema(0, DatabaseName, "ADMINISTRABLE_ROLE_AUTHORIZATIONS",
        [
            new ColumnDefinition("USER", DataType.VarChar, 32),
            new ColumnDefinition("HOST", DataType.VarChar, 255),
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("GRANTEE_HOST", DataType.VarChar, 255),
            new ColumnDefinition("ROLE_NAME", DataType.VarChar, 255),
            new ColumnDefinition("ROLE_HOST", DataType.VarChar, 255),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3),
            new ColumnDefinition("IS_DEFAULT", DataType.VarChar, 3),
            new ColumnDefinition("IS_MANDATORY", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetAdministrableRoleAuthorizations()
    {
        // Return empty result - CyscaleDB doesn't support roles yet
        return ResultSet.FromSchema(CreateAdministrableRoleAuthorizationsSchema());
    }

    #endregion

    #region APPLICABLE_ROLES

    private static TableSchema CreateApplicableRolesSchema()
    {
        return new TableSchema(0, DatabaseName, "APPLICABLE_ROLES",
        [
            new ColumnDefinition("USER", DataType.VarChar, 32),
            new ColumnDefinition("HOST", DataType.VarChar, 255),
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("GRANTEE_HOST", DataType.VarChar, 255),
            new ColumnDefinition("ROLE_NAME", DataType.VarChar, 255),
            new ColumnDefinition("ROLE_HOST", DataType.VarChar, 255),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3),
            new ColumnDefinition("IS_DEFAULT", DataType.VarChar, 3),
            new ColumnDefinition("IS_MANDATORY", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetApplicableRoles()
    {
        // Return empty result - CyscaleDB doesn't support roles yet
        return ResultSet.FromSchema(CreateApplicableRolesSchema());
    }

    #endregion

    #region ENABLED_ROLES

    private static TableSchema CreateEnabledRolesSchema()
    {
        return new TableSchema(0, DatabaseName, "ENABLED_ROLES",
        [
            new ColumnDefinition("ROLE_NAME", DataType.VarChar, 255),
            new ColumnDefinition("ROLE_HOST", DataType.VarChar, 255),
            new ColumnDefinition("IS_DEFAULT", DataType.VarChar, 3),
            new ColumnDefinition("IS_MANDATORY", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetEnabledRoles()
    {
        // Return empty result - CyscaleDB doesn't support roles yet
        return ResultSet.FromSchema(CreateEnabledRolesSchema());
    }

    #endregion

    #region ROLE_COLUMN_GRANTS

    private static TableSchema CreateRoleColumnGrantsSchema()
    {
        return new TableSchema(0, DatabaseName, "ROLE_COLUMN_GRANTS",
        [
            new ColumnDefinition("GRANTOR", DataType.VarChar, 292),
            new ColumnDefinition("GRANTOR_HOST", DataType.VarChar, 255),
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("GRANTEE_HOST", DataType.VarChar, 255),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLUMN_NAME", DataType.VarChar, 64),
            new ColumnDefinition("PRIVILEGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetRoleColumnGrants()
    {
        // Return empty result - CyscaleDB doesn't support roles yet
        return ResultSet.FromSchema(CreateRoleColumnGrantsSchema());
    }

    #endregion

    #region ROLE_ROUTINE_GRANTS

    private static TableSchema CreateRoleRoutineGrantsSchema()
    {
        return new TableSchema(0, DatabaseName, "ROLE_ROUTINE_GRANTS",
        [
            new ColumnDefinition("GRANTOR", DataType.VarChar, 292),
            new ColumnDefinition("GRANTOR_HOST", DataType.VarChar, 255),
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("GRANTEE_HOST", DataType.VarChar, 255),
            new ColumnDefinition("SPECIFIC_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("SPECIFIC_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("SPECIFIC_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ROUTINE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("ROUTINE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("ROUTINE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("PRIVILEGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetRoleRoutineGrants()
    {
        // Return empty result - CyscaleDB doesn't support roles yet
        return ResultSet.FromSchema(CreateRoleRoutineGrantsSchema());
    }

    #endregion

    #region ROLE_TABLE_GRANTS

    private static TableSchema CreateRoleTableGrantsSchema()
    {
        return new TableSchema(0, DatabaseName, "ROLE_TABLE_GRANTS",
        [
            new ColumnDefinition("GRANTOR", DataType.VarChar, 292),
            new ColumnDefinition("GRANTOR_HOST", DataType.VarChar, 255),
            new ColumnDefinition("GRANTEE", DataType.VarChar, 292),
            new ColumnDefinition("GRANTEE_HOST", DataType.VarChar, 255),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("PRIVILEGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("IS_GRANTABLE", DataType.VarChar, 3)
        ]);
    }

    private ResultSet GetRoleTableGrants()
    {
        // Return empty result - CyscaleDB doesn't support roles yet
        return ResultSet.FromSchema(CreateRoleTableGrantsSchema());
    }

    #endregion

    #region SCHEMATA_EXTENSIONS

    private static TableSchema CreateSchemataExtensionsSchema()
    {
        return new TableSchema(0, DatabaseName, "SCHEMATA_EXTENSIONS",
        [
            new ColumnDefinition("CATALOG_NAME", DataType.VarChar, 64),
            new ColumnDefinition("SCHEMA_NAME", DataType.VarChar, 64),
            new ColumnDefinition("OPTIONS", DataType.VarChar, 256)
        ]);
    }

    private ResultSet GetSchemataExtensions(string? filterSchema)
    {
        var result = ResultSet.FromSchema(CreateSchemataExtensionsSchema());

        foreach (var db in _catalog.Databases)
        {
            if (filterSchema != null && !db.Name.Equals(filterSchema, StringComparison.OrdinalIgnoreCase))
                continue;

            result.Rows.Add([
                DataValue.FromVarChar("def"),
                DataValue.FromVarChar(db.Name),
                DataValue.FromVarChar("")
            ]);
        }

        return result;
    }

    #endregion

    #region TABLES_EXTENSIONS

    private static TableSchema CreateTablesExtensionsSchema()
    {
        return new TableSchema(0, DatabaseName, "TABLES_EXTENSIONS",
        [
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ENGINE_ATTRIBUTE", DataType.Text),
            new ColumnDefinition("SECONDARY_ENGINE_ATTRIBUTE", DataType.Text)
        ]);
    }

    private ResultSet GetTablesExtensions(string? filterSchema, string? filterTable)
    {
        var result = ResultSet.FromSchema(CreateTablesExtensionsSchema());

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
                    DataValue.Null,
                    DataValue.Null
                ]);
            }
        }

        return result;
    }

    #endregion

    #region COLUMNS_EXTENSIONS

    private static TableSchema CreateColumnsExtensionsSchema()
    {
        return new TableSchema(0, DatabaseName, "COLUMNS_EXTENSIONS",
        [
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLUMN_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ENGINE_ATTRIBUTE", DataType.Text),
            new ColumnDefinition("SECONDARY_ENGINE_ATTRIBUTE", DataType.Text)
        ]);
    }

    private ResultSet GetColumnsExtensions(string? filterSchema, string? filterTable)
    {
        // Return empty result - not tracking column extensions
        return ResultSet.FromSchema(CreateColumnsExtensionsSchema());
    }

    #endregion

    #region TABLE_CONSTRAINTS_EXTENSIONS

    private static TableSchema CreateTableConstraintsExtensionsSchema()
    {
        return new TableSchema(0, DatabaseName, "TABLE_CONSTRAINTS_EXTENSIONS",
        [
            new ColumnDefinition("CONSTRAINT_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("CONSTRAINT_NAME", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ENGINE_ATTRIBUTE", DataType.Text),
            new ColumnDefinition("SECONDARY_ENGINE_ATTRIBUTE", DataType.Text)
        ]);
    }

    private ResultSet GetTableConstraintsExtensions(string? filterSchema)
    {
        // Return empty result
        return ResultSet.FromSchema(CreateTableConstraintsExtensionsSchema());
    }

    #endregion

    #region TABLESPACES

    private static TableSchema CreateTablespacesSchema()
    {
        return new TableSchema(0, DatabaseName, "TABLESPACES",
        [
            new ColumnDefinition("TABLESPACE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ENGINE", DataType.VarChar, 64),
            new ColumnDefinition("TABLESPACE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("LOGFILE_GROUP_NAME", DataType.VarChar, 64),
            new ColumnDefinition("EXTENT_SIZE", DataType.BigInt),
            new ColumnDefinition("AUTOEXTEND_SIZE", DataType.BigInt),
            new ColumnDefinition("MAXIMUM_SIZE", DataType.BigInt),
            new ColumnDefinition("NODEGROUP_ID", DataType.BigInt),
            new ColumnDefinition("TABLESPACE_COMMENT", DataType.VarChar, 2048)
        ]);
    }

    private ResultSet GetTablespaces()
    {
        // Return empty result - CyscaleDB doesn't support tablespaces
        return ResultSet.FromSchema(CreateTablespacesSchema());
    }

    #endregion

    #region TABLESPACES_EXTENSIONS

    private static TableSchema CreateTablespacesExtensionsSchema()
    {
        return new TableSchema(0, DatabaseName, "TABLESPACES_EXTENSIONS",
        [
            new ColumnDefinition("TABLESPACE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("ENGINE_ATTRIBUTE", DataType.Text)
        ]);
    }

    private ResultSet GetTablespacesExtensions()
    {
        // Return empty result
        return ResultSet.FromSchema(CreateTablespacesExtensionsSchema());
    }

    #endregion

    #region VIEW_ROUTINE_USAGE

    private static TableSchema CreateViewRoutineUsageSchema()
    {
        return new TableSchema(0, DatabaseName, "VIEW_ROUTINE_USAGE",
        [
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("SPECIFIC_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("SPECIFIC_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("SPECIFIC_NAME", DataType.VarChar, 64)
        ]);
    }

    private ResultSet GetViewRoutineUsage(string? filterSchema)
    {
        // Return empty result - not tracking view routine usage
        return ResultSet.FromSchema(CreateViewRoutineUsageSchema());
    }

    #endregion

    #region VIEW_TABLE_USAGE

    private static TableSchema CreateViewTableUsageSchema()
    {
        return new TableSchema(0, DatabaseName, "VIEW_TABLE_USAGE",
        [
            new ColumnDefinition("VIEW_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("VIEW_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("VIEW_NAME", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_CATALOG", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_SCHEMA", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64)
        ]);
    }

    private ResultSet GetViewTableUsage(string? filterSchema)
    {
        // Return empty result - not tracking view table usage
        return ResultSet.FromSchema(CreateViewTableUsageSchema());
    }

    #endregion

    #region COLUMN_STATISTICS

    private static TableSchema CreateColumnStatisticsSchema()
    {
        return new TableSchema(0, DatabaseName, "COLUMN_STATISTICS",
        [
            new ColumnDefinition("SCHEMA_NAME", DataType.VarChar, 64),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 64),
            new ColumnDefinition("COLUMN_NAME", DataType.VarChar, 64),
            new ColumnDefinition("HISTOGRAM", DataType.Text)
        ]);
    }

    private ResultSet GetColumnStatistics(string? filterSchema, string? filterTable)
    {
        // Return empty result - CyscaleDB doesn't track histogram statistics
        return ResultSet.FromSchema(CreateColumnStatisticsSchema());
    }

    #endregion

    #region KEYWORDS

    private static TableSchema CreateKeywordsSchema()
    {
        return new TableSchema(0, DatabaseName, "KEYWORDS",
        [
            new ColumnDefinition("WORD", DataType.VarChar, 128),
            new ColumnDefinition("RESERVED", DataType.Int)
        ]);
    }

    private ResultSet GetKeywords()
    {
        var result = ResultSet.FromSchema(CreateKeywordsSchema());

        // Return MySQL reserved keywords
        var reservedKeywords = new[]
        {
            "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "BEFORE", "BETWEEN", "BIGINT",
            "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER",
            "CHECK", "COLLATE", "COLUMN", "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE",
            "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
            "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND",
            "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE",
            "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH",
            "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH",
            "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "GENERATED",
            "GET", "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE",
            "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE",
            "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO",
            "IO_AFTER_GTIDS", "IO_BEFORE_GTIDS", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL",
            "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME",
            "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY",
            "MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT", "MATCH", "MAXVALUE", "MEDIUMBLOB",
            "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD",
            "MODIFIES", "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE",
            "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "PARTITION", "PRECISION",
            "PRIMARY", "PROCEDURE", "PURGE", "RANGE", "READ", "READS", "READ_WRITE", "REAL",
            "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESIGNAL",
            "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND",
            "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SMALLINT", "SPATIAL",
            "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT",
            "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STORED", "STRAIGHT_JOIN",
            "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING",
            "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE",
            "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR",
            "VARCHARACTER", "VARYING", "VIRTUAL", "WHEN", "WHERE", "WHILE", "WITH", "WRITE", "XOR",
            "YEAR_MONTH", "ZEROFILL"
        };

        foreach (var keyword in reservedKeywords)
        {
            result.Rows.Add([
                DataValue.FromVarChar(keyword),
                DataValue.FromInt(1)
            ]);
        }

        return result;
    }

    #endregion

    #region OPTIMIZER_TRACE

    private static TableSchema CreateOptimizerTraceSchema()
    {
        return new TableSchema(0, DatabaseName, "OPTIMIZER_TRACE",
        [
            new ColumnDefinition("QUERY", DataType.Text),
            new ColumnDefinition("TRACE", DataType.Text),
            new ColumnDefinition("MISSING_BYTES_BEYOND_MAX_MEM_SIZE", DataType.Int),
            new ColumnDefinition("INSUFFICIENT_PRIVILEGES", DataType.TinyInt)
        ]);
    }

    private ResultSet GetOptimizerTrace()
    {
        // Return empty result - optimizer tracing not implemented
        return ResultSet.FromSchema(CreateOptimizerTraceSchema());
    }

    #endregion

    #region PROFILING

    private static TableSchema CreateProfilingSchema()
    {
        return new TableSchema(0, DatabaseName, "PROFILING",
        [
            new ColumnDefinition("QUERY_ID", DataType.Int),
            new ColumnDefinition("SEQ", DataType.Int),
            new ColumnDefinition("STATE", DataType.VarChar, 30),
            new ColumnDefinition("DURATION", DataType.Decimal),
            new ColumnDefinition("CPU_USER", DataType.Decimal),
            new ColumnDefinition("CPU_SYSTEM", DataType.Decimal),
            new ColumnDefinition("CONTEXT_VOLUNTARY", DataType.Int),
            new ColumnDefinition("CONTEXT_INVOLUNTARY", DataType.Int),
            new ColumnDefinition("BLOCK_OPS_IN", DataType.Int),
            new ColumnDefinition("BLOCK_OPS_OUT", DataType.Int),
            new ColumnDefinition("MESSAGES_SENT", DataType.Int),
            new ColumnDefinition("MESSAGES_RECEIVED", DataType.Int),
            new ColumnDefinition("PAGE_FAULTS_MAJOR", DataType.Int),
            new ColumnDefinition("PAGE_FAULTS_MINOR", DataType.Int),
            new ColumnDefinition("SWAPS", DataType.Int),
            new ColumnDefinition("SOURCE_FUNCTION", DataType.VarChar, 30),
            new ColumnDefinition("SOURCE_FILE", DataType.VarChar, 20),
            new ColumnDefinition("SOURCE_LINE", DataType.Int)
        ]);
    }

    private ResultSet GetProfiling()
    {
        // Return empty result - profiling not implemented
        return ResultSet.FromSchema(CreateProfilingSchema());
    }

    #endregion

    #region RESOURCE_GROUPS

    private static TableSchema CreateResourceGroupsSchema()
    {
        return new TableSchema(0, DatabaseName, "RESOURCE_GROUPS",
        [
            new ColumnDefinition("RESOURCE_GROUP_NAME", DataType.VarChar, 64),
            new ColumnDefinition("RESOURCE_GROUP_TYPE", DataType.VarChar, 4),
            new ColumnDefinition("RESOURCE_GROUP_ENABLED", DataType.TinyInt),
            new ColumnDefinition("VCPU_IDS", DataType.Blob),
            new ColumnDefinition("THREAD_PRIORITY", DataType.Int)
        ]);
    }

    private ResultSet GetResourceGroups()
    {
        var result = ResultSet.FromSchema(CreateResourceGroupsSchema());

        // Return default resource groups
        result.Rows.Add([
            DataValue.FromVarChar("USR_default"),
            DataValue.FromVarChar("USER"),
            DataValue.FromInt(1),
            DataValue.Null,
            DataValue.FromInt(0)
        ]);

        result.Rows.Add([
            DataValue.FromVarChar("SYS_default"),
            DataValue.FromVarChar("SYSTEM"),
            DataValue.FromInt(1),
            DataValue.Null,
            DataValue.FromInt(0)
        ]);

        return result;
    }

    #endregion

    #region InnoDB Buffer Pool Tables

    private static TableSchema CreateInnodbBufferPageSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_BUFFER_PAGE",
        [
            new ColumnDefinition("POOL_ID", DataType.BigInt),
            new ColumnDefinition("BLOCK_ID", DataType.BigInt),
            new ColumnDefinition("SPACE", DataType.BigInt),
            new ColumnDefinition("PAGE_NUMBER", DataType.BigInt),
            new ColumnDefinition("PAGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("FLUSH_TYPE", DataType.BigInt),
            new ColumnDefinition("FIX_COUNT", DataType.BigInt),
            new ColumnDefinition("IS_HASHED", DataType.VarChar, 3),
            new ColumnDefinition("NEWEST_MODIFICATION", DataType.BigInt),
            new ColumnDefinition("OLDEST_MODIFICATION", DataType.BigInt),
            new ColumnDefinition("ACCESS_TIME", DataType.BigInt),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 1024),
            new ColumnDefinition("INDEX_NAME", DataType.VarChar, 1024),
            new ColumnDefinition("NUMBER_RECORDS", DataType.BigInt),
            new ColumnDefinition("DATA_SIZE", DataType.BigInt),
            new ColumnDefinition("COMPRESSED_SIZE", DataType.BigInt),
            new ColumnDefinition("PAGE_STATE", DataType.VarChar, 64),
            new ColumnDefinition("IO_FIX", DataType.VarChar, 64),
            new ColumnDefinition("IS_OLD", DataType.VarChar, 3),
            new ColumnDefinition("FREE_PAGE_CLOCK", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbBufferPage()
    {
        return ResultSet.FromSchema(CreateInnodbBufferPageSchema());
    }

    private static TableSchema CreateInnodbBufferPageLruSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_BUFFER_PAGE_LRU",
        [
            new ColumnDefinition("POOL_ID", DataType.BigInt),
            new ColumnDefinition("LRU_POSITION", DataType.BigInt),
            new ColumnDefinition("SPACE", DataType.BigInt),
            new ColumnDefinition("PAGE_NUMBER", DataType.BigInt),
            new ColumnDefinition("PAGE_TYPE", DataType.VarChar, 64),
            new ColumnDefinition("FLUSH_TYPE", DataType.BigInt),
            new ColumnDefinition("FIX_COUNT", DataType.BigInt),
            new ColumnDefinition("IS_HASHED", DataType.VarChar, 3),
            new ColumnDefinition("NEWEST_MODIFICATION", DataType.BigInt),
            new ColumnDefinition("OLDEST_MODIFICATION", DataType.BigInt),
            new ColumnDefinition("ACCESS_TIME", DataType.BigInt),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 1024),
            new ColumnDefinition("INDEX_NAME", DataType.VarChar, 1024),
            new ColumnDefinition("NUMBER_RECORDS", DataType.BigInt),
            new ColumnDefinition("DATA_SIZE", DataType.BigInt),
            new ColumnDefinition("COMPRESSED_SIZE", DataType.BigInt),
            new ColumnDefinition("COMPRESSED", DataType.VarChar, 3),
            new ColumnDefinition("IO_FIX", DataType.VarChar, 64),
            new ColumnDefinition("IS_OLD", DataType.VarChar, 3),
            new ColumnDefinition("FREE_PAGE_CLOCK", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbBufferPageLru()
    {
        return ResultSet.FromSchema(CreateInnodbBufferPageLruSchema());
    }

    private static TableSchema CreateInnodbBufferPoolStatsSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_BUFFER_POOL_STATS",
        [
            new ColumnDefinition("POOL_ID", DataType.BigInt),
            new ColumnDefinition("POOL_SIZE", DataType.BigInt),
            new ColumnDefinition("FREE_BUFFERS", DataType.BigInt),
            new ColumnDefinition("DATABASE_PAGES", DataType.BigInt),
            new ColumnDefinition("OLD_DATABASE_PAGES", DataType.BigInt),
            new ColumnDefinition("MODIFIED_DATABASE_PAGES", DataType.BigInt),
            new ColumnDefinition("PENDING_DECOMPRESS", DataType.BigInt),
            new ColumnDefinition("PENDING_READS", DataType.BigInt),
            new ColumnDefinition("PENDING_FLUSH_LRU", DataType.BigInt),
            new ColumnDefinition("PENDING_FLUSH_LIST", DataType.BigInt),
            new ColumnDefinition("PAGES_MADE_YOUNG", DataType.BigInt),
            new ColumnDefinition("PAGES_NOT_MADE_YOUNG", DataType.BigInt),
            new ColumnDefinition("PAGES_MADE_YOUNG_RATE", DataType.Double),
            new ColumnDefinition("PAGES_MADE_NOT_YOUNG_RATE", DataType.Double),
            new ColumnDefinition("NUMBER_PAGES_READ", DataType.BigInt),
            new ColumnDefinition("NUMBER_PAGES_CREATED", DataType.BigInt),
            new ColumnDefinition("NUMBER_PAGES_WRITTEN", DataType.BigInt),
            new ColumnDefinition("PAGES_READ_RATE", DataType.Double),
            new ColumnDefinition("PAGES_CREATE_RATE", DataType.Double),
            new ColumnDefinition("PAGES_WRITTEN_RATE", DataType.Double),
            new ColumnDefinition("NUMBER_PAGES_GET", DataType.BigInt),
            new ColumnDefinition("HIT_RATE", DataType.BigInt),
            new ColumnDefinition("YOUNG_MAKE_PER_THOUSAND_GETS", DataType.BigInt),
            new ColumnDefinition("NOT_YOUNG_MAKE_PER_THOUSAND_GETS", DataType.BigInt),
            new ColumnDefinition("NUMBER_PAGES_READ_AHEAD", DataType.BigInt),
            new ColumnDefinition("NUMBER_READ_AHEAD_EVICTED", DataType.BigInt),
            new ColumnDefinition("READ_AHEAD_RATE", DataType.Double),
            new ColumnDefinition("READ_AHEAD_EVICTED_RATE", DataType.Double),
            new ColumnDefinition("LRU_IO_TOTAL", DataType.BigInt),
            new ColumnDefinition("LRU_IO_CURRENT", DataType.BigInt),
            new ColumnDefinition("UNCOMPRESS_TOTAL", DataType.BigInt),
            new ColumnDefinition("UNCOMPRESS_CURRENT", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbBufferPoolStats()
    {
        var result = ResultSet.FromSchema(CreateInnodbBufferPoolStatsSchema());

        // Return single pool with basic stats
        result.Rows.Add([
            DataValue.FromBigInt(0),    // POOL_ID
            DataValue.FromBigInt(8192), // POOL_SIZE
            DataValue.FromBigInt(8000), // FREE_BUFFERS
            DataValue.FromBigInt(192),  // DATABASE_PAGES
            DataValue.FromBigInt(0),    // OLD_DATABASE_PAGES
            DataValue.FromBigInt(0),    // MODIFIED_DATABASE_PAGES
            DataValue.FromBigInt(0),    // PENDING_DECOMPRESS
            DataValue.FromBigInt(0),    // PENDING_READS
            DataValue.FromBigInt(0),    // PENDING_FLUSH_LRU
            DataValue.FromBigInt(0),    // PENDING_FLUSH_LIST
            DataValue.FromBigInt(0),    // PAGES_MADE_YOUNG
            DataValue.FromBigInt(0),    // PAGES_NOT_MADE_YOUNG
            DataValue.FromDouble(0),    // PAGES_MADE_YOUNG_RATE
            DataValue.FromDouble(0),    // PAGES_MADE_NOT_YOUNG_RATE
            DataValue.FromBigInt(0),    // NUMBER_PAGES_READ
            DataValue.FromBigInt(0),    // NUMBER_PAGES_CREATED
            DataValue.FromBigInt(0),    // NUMBER_PAGES_WRITTEN
            DataValue.FromDouble(0),    // PAGES_READ_RATE
            DataValue.FromDouble(0),    // PAGES_CREATE_RATE
            DataValue.FromDouble(0),    // PAGES_WRITTEN_RATE
            DataValue.FromBigInt(0),    // NUMBER_PAGES_GET
            DataValue.FromBigInt(1000), // HIT_RATE
            DataValue.FromBigInt(0),    // YOUNG_MAKE_PER_THOUSAND_GETS
            DataValue.FromBigInt(0),    // NOT_YOUNG_MAKE_PER_THOUSAND_GETS
            DataValue.FromBigInt(0),    // NUMBER_PAGES_READ_AHEAD
            DataValue.FromBigInt(0),    // NUMBER_READ_AHEAD_EVICTED
            DataValue.FromDouble(0),    // READ_AHEAD_RATE
            DataValue.FromDouble(0),    // READ_AHEAD_EVICTED_RATE
            DataValue.FromBigInt(0),    // LRU_IO_TOTAL
            DataValue.FromBigInt(0),    // LRU_IO_CURRENT
            DataValue.FromBigInt(0),    // UNCOMPRESS_TOTAL
            DataValue.FromBigInt(0)     // UNCOMPRESS_CURRENT
        ]);

        return result;
    }

    private static TableSchema CreateInnodbCachedIndexesSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_CACHED_INDEXES",
        [
            new ColumnDefinition("SPACE_ID", DataType.BigInt),
            new ColumnDefinition("INDEX_ID", DataType.BigInt),
            new ColumnDefinition("N_CACHED_PAGES", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbCachedIndexes()
    {
        return ResultSet.FromSchema(CreateInnodbCachedIndexesSchema());
    }

    #endregion

    #region InnoDB Compression Tables

    private static TableSchema CreateInnodbCmpSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_CMP",
        [
            new ColumnDefinition("PAGE_SIZE", DataType.Int),
            new ColumnDefinition("COMPRESS_OPS", DataType.Int),
            new ColumnDefinition("COMPRESS_OPS_OK", DataType.Int),
            new ColumnDefinition("COMPRESS_TIME", DataType.Int),
            new ColumnDefinition("UNCOMPRESS_OPS", DataType.Int),
            new ColumnDefinition("UNCOMPRESS_TIME", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbCmp()
    {
        return ResultSet.FromSchema(CreateInnodbCmpSchema());
    }

    private static TableSchema CreateInnodbCmpResetSchema()
    {
        return CreateInnodbCmpSchema();
    }

    private ResultSet GetInnodbCmpReset()
    {
        return ResultSet.FromSchema(CreateInnodbCmpResetSchema());
    }

    private static TableSchema CreateInnodbCmpPerIndexSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_CMP_PER_INDEX",
        [
            new ColumnDefinition("DATABASE_NAME", DataType.VarChar, 192),
            new ColumnDefinition("TABLE_NAME", DataType.VarChar, 192),
            new ColumnDefinition("INDEX_NAME", DataType.VarChar, 192),
            new ColumnDefinition("COMPRESS_OPS", DataType.Int),
            new ColumnDefinition("COMPRESS_OPS_OK", DataType.Int),
            new ColumnDefinition("COMPRESS_TIME", DataType.Int),
            new ColumnDefinition("UNCOMPRESS_OPS", DataType.Int),
            new ColumnDefinition("UNCOMPRESS_TIME", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbCmpPerIndex()
    {
        return ResultSet.FromSchema(CreateInnodbCmpPerIndexSchema());
    }

    private static TableSchema CreateInnodbCmpPerIndexResetSchema()
    {
        return CreateInnodbCmpPerIndexSchema();
    }

    private ResultSet GetInnodbCmpPerIndexReset()
    {
        return ResultSet.FromSchema(CreateInnodbCmpPerIndexResetSchema());
    }

    private static TableSchema CreateInnodbCmpmemSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_CMPMEM",
        [
            new ColumnDefinition("PAGE_SIZE", DataType.Int),
            new ColumnDefinition("BUFFER_POOL_INSTANCE", DataType.Int),
            new ColumnDefinition("PAGES_USED", DataType.Int),
            new ColumnDefinition("PAGES_FREE", DataType.Int),
            new ColumnDefinition("RELOCATION_OPS", DataType.BigInt),
            new ColumnDefinition("RELOCATION_TIME", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbCmpmem()
    {
        return ResultSet.FromSchema(CreateInnodbCmpmemSchema());
    }

    private static TableSchema CreateInnodbCmpmemResetSchema()
    {
        return CreateInnodbCmpmemSchema();
    }

    private ResultSet GetInnodbCmpmemReset()
    {
        return ResultSet.FromSchema(CreateInnodbCmpmemResetSchema());
    }

    #endregion

    #region InnoDB Metadata Tables

    private static TableSchema CreateInnodbColumnsSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_COLUMNS",
        [
            new ColumnDefinition("TABLE_ID", DataType.BigInt),
            new ColumnDefinition("NAME", DataType.VarChar, 193),
            new ColumnDefinition("POS", DataType.BigInt),
            new ColumnDefinition("MTYPE", DataType.Int),
            new ColumnDefinition("PRTYPE", DataType.Int),
            new ColumnDefinition("LEN", DataType.Int),
            new ColumnDefinition("HAS_DEFAULT", DataType.Int),
            new ColumnDefinition("DEFAULT_VALUE", DataType.Text)
        ]);
    }

    private ResultSet GetInnodbColumns()
    {
        var result = ResultSet.FromSchema(CreateInnodbColumnsSchema());

        foreach (var db in _catalog.Databases)
        {
            foreach (var table in db.Tables)
            {
                int pos = 0;
                foreach (var col in table.Columns)
                {
                    result.Rows.Add([
                        DataValue.FromBigInt(table.TableId),
                        DataValue.FromVarChar(col.Name),
                        DataValue.FromBigInt(pos++),
                        DataValue.FromInt(GetMType(col.DataType)),
                        DataValue.FromInt(0),
                        DataValue.FromInt(col.MaxLength > 0 ? col.MaxLength : GetDefaultLength(col.DataType)),
                        DataValue.FromInt(col.DefaultValue.HasValue ? 1 : 0),
                        col.DefaultValue.HasValue ? DataValue.FromVarChar(col.DefaultValue.Value.ToString()) : DataValue.Null
                    ]);
                }
            }
        }

        return result;
    }

    private static int GetMType(DataType dataType)
    {
        return dataType switch
        {
            DataType.VarChar or DataType.Char or DataType.Text => 1,
            DataType.Int or DataType.BigInt or DataType.SmallInt or DataType.TinyInt => 6,
            DataType.Float or DataType.Double => 3,
            DataType.Decimal => 4,
            DataType.DateTime or DataType.Timestamp => 5,
            DataType.Blob => 5,
            _ => 1
        };
    }

    private static int GetDefaultLength(DataType dataType)
    {
        return dataType switch
        {
            DataType.TinyInt => 1,
            DataType.SmallInt => 2,
            DataType.Int => 4,
            DataType.BigInt => 8,
            DataType.Float => 4,
            DataType.Double => 8,
            _ => 0
        };
    }

    private static TableSchema CreateInnodbDatafilesSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_DATAFILES",
        [
            new ColumnDefinition("SPACE", DataType.BigInt),
            new ColumnDefinition("PATH", DataType.VarChar, 4000)
        ]);
    }

    private ResultSet GetInnodbDatafiles()
    {
        return ResultSet.FromSchema(CreateInnodbDatafilesSchema());
    }

    private static TableSchema CreateInnodbFieldsSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FIELDS",
        [
            new ColumnDefinition("INDEX_ID", DataType.BigInt),
            new ColumnDefinition("NAME", DataType.VarChar, 193),
            new ColumnDefinition("POS", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbFields()
    {
        return ResultSet.FromSchema(CreateInnodbFieldsSchema());
    }

    private static TableSchema CreateInnodbForeignSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FOREIGN",
        [
            new ColumnDefinition("ID", DataType.VarChar, 129),
            new ColumnDefinition("FOR_NAME", DataType.VarChar, 129),
            new ColumnDefinition("REF_NAME", DataType.VarChar, 129),
            new ColumnDefinition("N_COLS", DataType.Int),
            new ColumnDefinition("TYPE", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbForeign()
    {
        return ResultSet.FromSchema(CreateInnodbForeignSchema());
    }

    private static TableSchema CreateInnodbForeignColsSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FOREIGN_COLS",
        [
            new ColumnDefinition("ID", DataType.VarChar, 129),
            new ColumnDefinition("FOR_COL_NAME", DataType.VarChar, 193),
            new ColumnDefinition("REF_COL_NAME", DataType.VarChar, 193),
            new ColumnDefinition("POS", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbForeignCols()
    {
        return ResultSet.FromSchema(CreateInnodbForeignColsSchema());
    }

    private static TableSchema CreateInnodbIndexesSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_INDEXES",
        [
            new ColumnDefinition("INDEX_ID", DataType.BigInt),
            new ColumnDefinition("NAME", DataType.VarChar, 193),
            new ColumnDefinition("TABLE_ID", DataType.BigInt),
            new ColumnDefinition("TYPE", DataType.Int),
            new ColumnDefinition("N_FIELDS", DataType.Int),
            new ColumnDefinition("PAGE_NO", DataType.Int),
            new ColumnDefinition("SPACE", DataType.Int),
            new ColumnDefinition("MERGE_THRESHOLD", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbIndexes()
    {
        return ResultSet.FromSchema(CreateInnodbIndexesSchema());
    }

    private static TableSchema CreateInnodbTablesSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_TABLES",
        [
            new ColumnDefinition("TABLE_ID", DataType.BigInt),
            new ColumnDefinition("NAME", DataType.VarChar, 655),
            new ColumnDefinition("FLAG", DataType.Int),
            new ColumnDefinition("N_COLS", DataType.Int),
            new ColumnDefinition("SPACE", DataType.BigInt),
            new ColumnDefinition("ROW_FORMAT", DataType.VarChar, 12),
            new ColumnDefinition("ZIP_PAGE_SIZE", DataType.Int),
            new ColumnDefinition("SPACE_TYPE", DataType.VarChar, 10),
            new ColumnDefinition("INSTANT_COLS", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbTables()
    {
        var result = ResultSet.FromSchema(CreateInnodbTablesSchema());

        foreach (var db in _catalog.Databases)
        {
            foreach (var table in db.Tables)
            {
                result.Rows.Add([
                    DataValue.FromBigInt(table.TableId),
                    DataValue.FromVarChar($"{db.Name}/{table.TableName}"),
                    DataValue.FromInt(33),
                    DataValue.FromInt(table.Columns.Count),
                    DataValue.FromBigInt(table.TableId),
                    DataValue.FromVarChar("Dynamic"),
                    DataValue.FromInt(0),
                    DataValue.FromVarChar("Single"),
                    DataValue.FromInt(0)
                ]);
            }
        }

        return result;
    }

    #endregion

    #region InnoDB Full-text Tables

    private static TableSchema CreateInnodbFtBeingDeletedSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FT_BEING_DELETED",
        [
            new ColumnDefinition("DOC_ID", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbFtBeingDeleted()
    {
        return ResultSet.FromSchema(CreateInnodbFtBeingDeletedSchema());
    }

    private static TableSchema CreateInnodbFtConfigSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FT_CONFIG",
        [
            new ColumnDefinition("KEY", DataType.VarChar, 193),
            new ColumnDefinition("VALUE", DataType.VarChar, 193)
        ]);
    }

    private ResultSet GetInnodbFtConfig()
    {
        return ResultSet.FromSchema(CreateInnodbFtConfigSchema());
    }

    private static TableSchema CreateInnodbFtDefaultStopwordSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FT_DEFAULT_STOPWORD",
        [
            new ColumnDefinition("value", DataType.VarChar, 18)
        ]);
    }

    private ResultSet GetInnodbFtDefaultStopword()
    {
        var result = ResultSet.FromSchema(CreateInnodbFtDefaultStopwordSchema());

        // Common English stopwords
        var stopwords = new[] { "a", "about", "an", "are", "as", "at", "be", "by", "com", "de", "en",
                                 "for", "from", "how", "i", "in", "is", "it", "la", "of", "on", "or",
                                 "that", "the", "this", "to", "was", "what", "when", "where", "who",
                                 "will", "with", "und", "www" };

        foreach (var word in stopwords)
        {
            result.Rows.Add([DataValue.FromVarChar(word)]);
        }

        return result;
    }

    private static TableSchema CreateInnodbFtDeletedSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FT_DELETED",
        [
            new ColumnDefinition("DOC_ID", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbFtDeleted()
    {
        return ResultSet.FromSchema(CreateInnodbFtDeletedSchema());
    }

    private static TableSchema CreateInnodbFtIndexCacheSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FT_INDEX_CACHE",
        [
            new ColumnDefinition("WORD", DataType.VarChar, 337),
            new ColumnDefinition("FIRST_DOC_ID", DataType.BigInt),
            new ColumnDefinition("LAST_DOC_ID", DataType.BigInt),
            new ColumnDefinition("DOC_COUNT", DataType.BigInt),
            new ColumnDefinition("DOC_ID", DataType.BigInt),
            new ColumnDefinition("POSITION", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbFtIndexCache()
    {
        return ResultSet.FromSchema(CreateInnodbFtIndexCacheSchema());
    }

    private static TableSchema CreateInnodbFtIndexTableSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_FT_INDEX_TABLE",
        [
            new ColumnDefinition("WORD", DataType.VarChar, 337),
            new ColumnDefinition("FIRST_DOC_ID", DataType.BigInt),
            new ColumnDefinition("LAST_DOC_ID", DataType.BigInt),
            new ColumnDefinition("DOC_COUNT", DataType.BigInt),
            new ColumnDefinition("DOC_ID", DataType.BigInt),
            new ColumnDefinition("POSITION", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbFtIndexTable()
    {
        return ResultSet.FromSchema(CreateInnodbFtIndexTableSchema());
    }

    #endregion

    #region InnoDB Tablespace and Transaction Tables

    private static TableSchema CreateInnodbMetricsSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_METRICS",
        [
            new ColumnDefinition("NAME", DataType.VarChar, 193),
            new ColumnDefinition("SUBSYSTEM", DataType.VarChar, 193),
            new ColumnDefinition("COUNT", DataType.BigInt),
            new ColumnDefinition("MAX_COUNT", DataType.BigInt),
            new ColumnDefinition("MIN_COUNT", DataType.BigInt),
            new ColumnDefinition("AVG_COUNT", DataType.Double),
            new ColumnDefinition("COUNT_RESET", DataType.BigInt),
            new ColumnDefinition("MAX_COUNT_RESET", DataType.BigInt),
            new ColumnDefinition("MIN_COUNT_RESET", DataType.BigInt),
            new ColumnDefinition("AVG_COUNT_RESET", DataType.Double),
            new ColumnDefinition("TIME_ENABLED", DataType.DateTime),
            new ColumnDefinition("TIME_DISABLED", DataType.DateTime),
            new ColumnDefinition("TIME_ELAPSED", DataType.BigInt),
            new ColumnDefinition("TIME_RESET", DataType.DateTime),
            new ColumnDefinition("STATUS", DataType.VarChar, 8),
            new ColumnDefinition("TYPE", DataType.VarChar, 16),
            new ColumnDefinition("COMMENT", DataType.VarChar, 193)
        ]);
    }

    private ResultSet GetInnodbMetrics()
    {
        return ResultSet.FromSchema(CreateInnodbMetricsSchema());
    }

    private static TableSchema CreateInnodbSessionTempTablespacesSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_SESSION_TEMP_TABLESPACES",
        [
            new ColumnDefinition("ID", DataType.Int),
            new ColumnDefinition("SPACE", DataType.Int),
            new ColumnDefinition("PATH", DataType.VarChar, 4001),
            new ColumnDefinition("SIZE", DataType.BigInt),
            new ColumnDefinition("STATE", DataType.VarChar, 192),
            new ColumnDefinition("PURPOSE", DataType.VarChar, 192)
        ]);
    }

    private ResultSet GetInnodbSessionTempTablespaces()
    {
        return ResultSet.FromSchema(CreateInnodbSessionTempTablespacesSchema());
    }

    private static TableSchema CreateInnodbTablespacesSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_TABLESPACES",
        [
            new ColumnDefinition("SPACE", DataType.BigInt),
            new ColumnDefinition("NAME", DataType.VarChar, 655),
            new ColumnDefinition("FLAG", DataType.Int),
            new ColumnDefinition("ROW_FORMAT", DataType.VarChar, 22),
            new ColumnDefinition("PAGE_SIZE", DataType.Int),
            new ColumnDefinition("ZIP_PAGE_SIZE", DataType.Int),
            new ColumnDefinition("SPACE_TYPE", DataType.VarChar, 10),
            new ColumnDefinition("FS_BLOCK_SIZE", DataType.Int),
            new ColumnDefinition("FILE_SIZE", DataType.BigInt),
            new ColumnDefinition("ALLOCATED_SIZE", DataType.BigInt),
            new ColumnDefinition("AUTOEXTEND_SIZE", DataType.BigInt),
            new ColumnDefinition("SERVER_VERSION", DataType.VarChar, 10),
            new ColumnDefinition("SPACE_VERSION", DataType.Int),
            new ColumnDefinition("ENCRYPTION", DataType.VarChar, 1),
            new ColumnDefinition("STATE", DataType.VarChar, 10)
        ]);
    }

    private ResultSet GetInnodbTablespaces()
    {
        return ResultSet.FromSchema(CreateInnodbTablespacesSchema());
    }

    private static TableSchema CreateInnodbTablespacesBriefSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_TABLESPACES_BRIEF",
        [
            new ColumnDefinition("SPACE", DataType.BigInt),
            new ColumnDefinition("NAME", DataType.VarChar, 655),
            new ColumnDefinition("PATH", DataType.VarChar, 4001),
            new ColumnDefinition("FLAG", DataType.Int),
            new ColumnDefinition("SPACE_TYPE", DataType.VarChar, 10)
        ]);
    }

    private ResultSet GetInnodbTablespacesBrief()
    {
        return ResultSet.FromSchema(CreateInnodbTablespacesBriefSchema());
    }

    private static TableSchema CreateInnodbTablestatsSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_TABLESTATS",
        [
            new ColumnDefinition("TABLE_ID", DataType.BigInt),
            new ColumnDefinition("NAME", DataType.VarChar, 193),
            new ColumnDefinition("STATS_INITIALIZED", DataType.VarChar, 18),
            new ColumnDefinition("NUM_ROWS", DataType.BigInt),
            new ColumnDefinition("CLUST_INDEX_SIZE", DataType.BigInt),
            new ColumnDefinition("OTHER_INDEX_SIZE", DataType.BigInt),
            new ColumnDefinition("MODIFIED_COUNTER", DataType.BigInt),
            new ColumnDefinition("AUTOINC", DataType.BigInt),
            new ColumnDefinition("REF_COUNT", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbTablestats()
    {
        var result = ResultSet.FromSchema(CreateInnodbTablestatsSchema());

        foreach (var db in _catalog.Databases)
        {
            foreach (var table in db.Tables)
            {
                result.Rows.Add([
                    DataValue.FromBigInt(table.TableId),
                    DataValue.FromVarChar($"{db.Name}/{table.TableName}"),
                    DataValue.FromVarChar("Initialized"),
                    DataValue.FromBigInt(table.RowCount),
                    DataValue.FromBigInt(1),
                    DataValue.FromBigInt(0),
                    DataValue.FromBigInt(0),
                    DataValue.FromBigInt(table.GetNextAutoIncrementValue()),
                    DataValue.FromInt(0)
                ]);
            }
        }

        return result;
    }

    private static TableSchema CreateInnodbTempTableInfoSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_TEMP_TABLE_INFO",
        [
            new ColumnDefinition("TABLE_ID", DataType.BigInt),
            new ColumnDefinition("NAME", DataType.VarChar, 64),
            new ColumnDefinition("N_COLS", DataType.Int),
            new ColumnDefinition("SPACE", DataType.Int)
        ]);
    }

    private ResultSet GetInnodbTempTableInfo()
    {
        return ResultSet.FromSchema(CreateInnodbTempTableInfoSchema());
    }

    private static TableSchema CreateInnodbTrxSchema()
    {
        return new TableSchema(0, DatabaseName, "INNODB_TRX",
        [
            new ColumnDefinition("TRX_ID", DataType.VarChar, 18),
            new ColumnDefinition("TRX_STATE", DataType.VarChar, 13),
            new ColumnDefinition("TRX_STARTED", DataType.DateTime),
            new ColumnDefinition("TRX_REQUESTED_LOCK_ID", DataType.VarChar, 105),
            new ColumnDefinition("TRX_WAIT_STARTED", DataType.DateTime),
            new ColumnDefinition("TRX_WEIGHT", DataType.BigInt),
            new ColumnDefinition("TRX_MYSQL_THREAD_ID", DataType.BigInt),
            new ColumnDefinition("TRX_QUERY", DataType.VarChar, 1024),
            new ColumnDefinition("TRX_OPERATION_STATE", DataType.VarChar, 64),
            new ColumnDefinition("TRX_TABLES_IN_USE", DataType.BigInt),
            new ColumnDefinition("TRX_TABLES_LOCKED", DataType.BigInt),
            new ColumnDefinition("TRX_LOCK_STRUCTS", DataType.BigInt),
            new ColumnDefinition("TRX_LOCK_MEMORY_BYTES", DataType.BigInt),
            new ColumnDefinition("TRX_ROWS_LOCKED", DataType.BigInt),
            new ColumnDefinition("TRX_ROWS_MODIFIED", DataType.BigInt),
            new ColumnDefinition("TRX_CONCURRENCY_TICKETS", DataType.BigInt),
            new ColumnDefinition("TRX_ISOLATION_LEVEL", DataType.VarChar, 16),
            new ColumnDefinition("TRX_UNIQUE_CHECKS", DataType.Int),
            new ColumnDefinition("TRX_FOREIGN_KEY_CHECKS", DataType.Int),
            new ColumnDefinition("TRX_LAST_FOREIGN_KEY_ERROR", DataType.VarChar, 256),
            new ColumnDefinition("TRX_ADAPTIVE_HASH_LATCHED", DataType.Int),
            new ColumnDefinition("TRX_ADAPTIVE_HASH_TIMEOUT", DataType.BigInt),
            new ColumnDefinition("TRX_IS_READ_ONLY", DataType.Int),
            new ColumnDefinition("TRX_AUTOCOMMIT_NON_LOCKING", DataType.Int),
            new ColumnDefinition("TRX_SCHEDULE_WEIGHT", DataType.BigInt)
        ]);
    }

    private ResultSet GetInnodbTrx()
    {
        // Return empty result - transaction info would require access to transaction manager
        return ResultSet.FromSchema(CreateInnodbTrxSchema());
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
