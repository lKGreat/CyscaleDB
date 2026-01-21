using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.InformationSchema;
using Xunit;

namespace CyscaleDB.Tests;

/// <summary>
/// Tests for information_schema virtual tables.
/// </summary>
public class InformationSchemaTests
{
    private readonly Catalog _catalog;
    private readonly InformationSchemaProvider _provider;

    public InformationSchemaTests()
    {
        var dataPath = Path.Combine(Path.GetTempPath(), $"cyscale_test_{Guid.NewGuid()}");
        Directory.CreateDirectory(dataPath);
        _catalog = new Catalog(dataPath);
        _provider = new InformationSchemaProvider(_catalog);
    }

    [Fact]
    public void SupportedTables_ContainsAllRequiredTables()
    {
        // Core tables
        Assert.Contains("SCHEMATA", InformationSchemaProvider.SupportedTables);
        Assert.Contains("TABLES", InformationSchemaProvider.SupportedTables);
        Assert.Contains("COLUMNS", InformationSchemaProvider.SupportedTables);
        Assert.Contains("STATISTICS", InformationSchemaProvider.SupportedTables);

        // Character set tables
        Assert.Contains("CHARACTER_SETS", InformationSchemaProvider.SupportedTables);
        Assert.Contains("COLLATIONS", InformationSchemaProvider.SupportedTables);
        Assert.Contains("COLLATION_CHARACTER_SET_APPLICABILITY", InformationSchemaProvider.SupportedTables);

        // Constraint tables
        Assert.Contains("TABLE_CONSTRAINTS", InformationSchemaProvider.SupportedTables);
        Assert.Contains("CHECK_CONSTRAINTS", InformationSchemaProvider.SupportedTables);

        // System tables
        Assert.Contains("PROCESSLIST", InformationSchemaProvider.SupportedTables);
        Assert.Contains("PLUGINS", InformationSchemaProvider.SupportedTables);
        Assert.Contains("KEYWORDS", InformationSchemaProvider.SupportedTables);

        // InnoDB tables
        Assert.Contains("INNODB_BUFFER_POOL_STATS", InformationSchemaProvider.SupportedTables);
        Assert.Contains("INNODB_TABLES", InformationSchemaProvider.SupportedTables);
        Assert.Contains("INNODB_COLUMNS", InformationSchemaProvider.SupportedTables);
        Assert.Contains("INNODB_TRX", InformationSchemaProvider.SupportedTables);
    }

    [Fact]
    public void GetCharacterSets_ReturnsCommonCharsets()
    {
        var result = _provider.GetTableData("CHARACTER_SETS");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count > 0);

        // Check for utf8mb4
        var hasUtf8mb4 = result.Rows.Any(r => 
            r[0].ToString() == "utf8mb4");
        Assert.True(hasUtf8mb4);

        // Check for latin1
        var hasLatin1 = result.Rows.Any(r => 
            r[0].ToString() == "latin1");
        Assert.True(hasLatin1);
    }

    [Fact]
    public void GetCollations_ReturnsCommonCollations()
    {
        var result = _provider.GetTableData("COLLATIONS");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count > 0);

        // Check for utf8mb4_general_ci
        var hasUtf8mb4GeneralCi = result.Rows.Any(r => 
            r[0].ToString() == "utf8mb4_general_ci");
        Assert.True(hasUtf8mb4GeneralCi);
    }

    [Fact]
    public void GetKeywords_ReturnsReservedKeywords()
    {
        var result = _provider.GetTableData("KEYWORDS");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count > 100); // MySQL has many reserved keywords

        // Check for common keywords
        var hasSelect = result.Rows.Any(r => r[0].ToString() == "SELECT");
        var hasFrom = result.Rows.Any(r => r[0].ToString() == "FROM");
        var hasWhere = result.Rows.Any(r => r[0].ToString() == "WHERE");

        Assert.True(hasSelect);
        Assert.True(hasFrom);
        Assert.True(hasWhere);
    }

    [Fact]
    public void GetProcesslist_ReturnsCurrentProcess()
    {
        var result = _provider.GetTableData("PROCESSLIST");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count >= 1);

        // Check for expected columns
        var schema = InformationSchemaProvider.GetTableSchema("PROCESSLIST");
        Assert.Contains(schema.Columns, c => c.Name == "ID");
        Assert.Contains(schema.Columns, c => c.Name == "USER");
        Assert.Contains(schema.Columns, c => c.Name == "HOST");
        Assert.Contains(schema.Columns, c => c.Name == "COMMAND");
    }

    [Fact]
    public void GetPlugins_ReturnsCyscaleDbPlugin()
    {
        var result = _provider.GetTableData("PLUGINS");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count >= 1);

        // Check for CyscaleDB plugin
        var hasCyscaleDb = result.Rows.Any(r => 
            r[0].ToString() == "CyscaleDB");
        Assert.True(hasCyscaleDb);
    }

    [Fact]
    public void GetEngines_ReturnsCyscaleDbEngine()
    {
        var result = _provider.GetTableData("ENGINES");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count >= 1);

        var firstRow = result.Rows[0];
        Assert.Equal("CyscaleDB", firstRow[0].ToString());
        Assert.Equal("DEFAULT", firstRow[1].ToString());
    }

    [Fact]
    public void GetUserPrivileges_ReturnsRootUserPrivileges()
    {
        var result = _provider.GetTableData("USER_PRIVILEGES");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count > 0);

        // Check for SELECT privilege
        var hasSelectPriv = result.Rows.Any(r => 
            r[2].ToString() == "SELECT");
        Assert.True(hasSelectPriv);
    }

    [Fact]
    public void GetInnodbBufferPoolStats_ReturnsStats()
    {
        var result = _provider.GetTableData("INNODB_BUFFER_POOL_STATS");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count >= 1);

        var schema = InformationSchemaProvider.GetTableSchema("INNODB_BUFFER_POOL_STATS");
        Assert.Contains(schema.Columns, c => c.Name == "POOL_ID");
        Assert.Contains(schema.Columns, c => c.Name == "POOL_SIZE");
        Assert.Contains(schema.Columns, c => c.Name == "FREE_BUFFERS");
    }

    [Fact]
    public void GetResourceGroups_ReturnsDefaultGroups()
    {
        var result = _provider.GetTableData("RESOURCE_GROUPS");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count >= 2);

        // Check for default groups
        var hasUsrDefault = result.Rows.Any(r => 
            r[0].ToString() == "USR_default");
        var hasSysDefault = result.Rows.Any(r => 
            r[0].ToString() == "SYS_default");

        Assert.True(hasUsrDefault);
        Assert.True(hasSysDefault);
    }

    [Fact]
    public void GetInnodbFtDefaultStopword_ReturnsStopwords()
    {
        var result = _provider.GetTableData("INNODB_FT_DEFAULT_STOPWORD");

        Assert.NotNull(result);
        Assert.True(result.Rows.Count > 0);

        // Check for common stopwords
        var hasThe = result.Rows.Any(r => r[0].ToString() == "the");
        var hasAnd = result.Rows.Any(r => r[0].ToString() == "a");

        Assert.True(hasThe);
        Assert.True(hasAnd);
    }

    [Fact]
    public void GetTableSchema_ReturnsCorrectSchema_ForTables()
    {
        var schema = InformationSchemaProvider.GetTableSchema("TABLES");

        Assert.NotNull(schema);
        Assert.Equal("TABLES", schema.TableName);
        Assert.Equal("information_schema", schema.DatabaseName);
        
        // MySQL 8.0 TABLES table should have 21 columns
        Assert.Equal(21, schema.Columns.Count);

        // Check for specific columns
        Assert.Contains(schema.Columns, c => c.Name == "TABLE_CATALOG");
        Assert.Contains(schema.Columns, c => c.Name == "TABLE_SCHEMA");
        Assert.Contains(schema.Columns, c => c.Name == "TABLE_NAME");
        Assert.Contains(schema.Columns, c => c.Name == "ENGINE");
        Assert.Contains(schema.Columns, c => c.Name == "ROW_FORMAT");
        Assert.Contains(schema.Columns, c => c.Name == "TABLE_ROWS");
        Assert.Contains(schema.Columns, c => c.Name == "AUTO_INCREMENT");
        Assert.Contains(schema.Columns, c => c.Name == "CREATE_TIME");
    }

    [Fact]
    public void GetTableSchema_ReturnsCorrectSchema_ForColumns()
    {
        var schema = InformationSchemaProvider.GetTableSchema("COLUMNS");

        Assert.NotNull(schema);
        Assert.Equal("COLUMNS", schema.TableName);
        
        // MySQL 8.0 COLUMNS table should have 22 columns
        Assert.Equal(22, schema.Columns.Count);

        // Check for specific columns
        Assert.Contains(schema.Columns, c => c.Name == "CHARACTER_MAXIMUM_LENGTH");
        Assert.Contains(schema.Columns, c => c.Name == "NUMERIC_PRECISION");
        Assert.Contains(schema.Columns, c => c.Name == "DATETIME_PRECISION");
        Assert.Contains(schema.Columns, c => c.Name == "GENERATION_EXPRESSION");
    }

    [Fact]
    public void IsValidTable_ReturnsTrueForSupportedTables()
    {
        Assert.True(InformationSchemaProvider.IsValidTable("SCHEMATA"));
        Assert.True(InformationSchemaProvider.IsValidTable("TABLES"));
        Assert.True(InformationSchemaProvider.IsValidTable("COLUMNS"));
        Assert.True(InformationSchemaProvider.IsValidTable("CHARACTER_SETS"));
        Assert.True(InformationSchemaProvider.IsValidTable("INNODB_TRX"));
    }

    [Fact]
    public void IsValidTable_ReturnsFalseForUnsupportedTables()
    {
        Assert.False(InformationSchemaProvider.IsValidTable("NONEXISTENT_TABLE"));
        Assert.False(InformationSchemaProvider.IsValidTable(""));
        Assert.False(InformationSchemaProvider.IsValidTable("random"));
    }

    [Fact]
    public void IsValidTable_IsCaseInsensitive()
    {
        Assert.True(InformationSchemaProvider.IsValidTable("TABLES"));
        Assert.True(InformationSchemaProvider.IsValidTable("tables"));
        Assert.True(InformationSchemaProvider.IsValidTable("Tables"));
        Assert.True(InformationSchemaProvider.IsValidTable("TaBlEs"));
    }

    [Fact]
    public void DatabaseName_ReturnsInformationSchema()
    {
        Assert.Equal("information_schema", InformationSchemaProvider.DatabaseName);
    }

    [Fact]
    public void SupportedTables_HasExpectedCount()
    {
        // We should have 70+ tables for MySQL 8.0 compatibility
        Assert.True(InformationSchemaProvider.SupportedTables.Length >= 60);
    }

    [Theory]
    [InlineData("SCHEMATA")]
    [InlineData("TABLES")]
    [InlineData("COLUMNS")]
    [InlineData("CHARACTER_SETS")]
    [InlineData("COLLATIONS")]
    [InlineData("TABLE_CONSTRAINTS")]
    [InlineData("PROCESSLIST")]
    [InlineData("PLUGINS")]
    [InlineData("INNODB_BUFFER_POOL_STATS")]
    [InlineData("INNODB_TABLES")]
    [InlineData("INNODB_TRX")]
    public void GetTableData_DoesNotThrow_ForValidTables(string tableName)
    {
        var exception = Record.Exception(() => _provider.GetTableData(tableName));
        Assert.Null(exception);
    }

    [Theory]
    [InlineData("SCHEMATA")]
    [InlineData("TABLES")]
    [InlineData("COLUMNS")]
    [InlineData("CHARACTER_SETS")]
    [InlineData("COLLATIONS")]
    [InlineData("TABLE_CONSTRAINTS")]
    [InlineData("PROCESSLIST")]
    [InlineData("PLUGINS")]
    [InlineData("INNODB_BUFFER_POOL_STATS")]
    [InlineData("INNODB_TABLES")]
    [InlineData("INNODB_TRX")]
    public void GetTableSchema_DoesNotThrow_ForValidTables(string tableName)
    {
        var exception = Record.Exception(() => InformationSchemaProvider.GetTableSchema(tableName));
        Assert.Null(exception);
    }
}
