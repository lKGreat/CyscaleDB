using CyscaleDB.Core.Common;
using CyscaleDB.Core.Execution.Optimizer;
using CyscaleDB.Core.Execution.Operators;
using CyscaleDB.Core.Parsing;
using CyscaleDB.Core.Parsing.Ast;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Index;

namespace CyscaleDB.Tests;

public class IndexSelectorTests : IDisposable
{
    private readonly string _testDir;
    private readonly Catalog _catalog;
    private readonly IndexManager _indexManager;

    public IndexSelectorTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"CyscaleDB_IndexSelectorTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);

        _catalog = new Catalog(_testDir);
        _catalog.Initialize();
        _catalog.CreateDatabase("testdb");

        _indexManager = new IndexManager(_testDir);
    }

    public void Dispose()
    {
        _indexManager.Dispose();
        _catalog.Dispose();

        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    private TableSchema SetupTestTable()
    {
        // Create a test table schema
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, isNullable: false, isPrimaryKey: true),
            new("name", DataType.VarChar, 100),
            new("age", DataType.Int),
            new("status", DataType.VarChar, 50)
        };
        
        // Create table through catalog (which handles schema storage)
        return _catalog.CreateTable("testdb", "users", columns);
    }

    private Expression ParseWhereClause(string whereClause)
    {
        var parser = new Parser($"SELECT * FROM users WHERE {whereClause}");
        var stmt = parser.Parse() as SelectStatement;
        return stmt!.Where!;
    }

    #region Basic Selection Tests

    [Fact]
    public void SelectIndex_ReturnsNull_WhenNoIndexes()
    {
        SetupTestTable();
        var selector = new IndexSelector(_catalog, _indexManager);

        var whereClause = ParseWhereClause("id = 1");
        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.Null(result);
    }

    [Fact]
    public void SelectIndex_ReturnsNull_WhenNoWhereClause()
    {
        SetupTestTable();
        var selector = new IndexSelector(_catalog, _indexManager);

        var result = selector.SelectIndex("testdb", "users", null);

        Assert.Null(result);
    }

    [Fact]
    public void SelectIndex_SelectsMatchingIndex_ForEquality()
    {
        var schema = SetupTestTable();

        // Create an index on 'id'
        _indexManager.CreateIndex("idx_id", "users", "testdb", IndexType.BTree, ["id"], isUnique: true, schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("id = 5");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.NotNull(result);
        Assert.Equal("idx_id", result!.IndexInfo.IndexName);
        Assert.True(result.ScanRange.IsPointLookup);
    }

    [Fact]
    public void SelectIndex_SelectsBestIndex_WithMultipleIndexes()
    {
        var schema = SetupTestTable();

        // Create an index on 'name'
        _indexManager.CreateIndex("idx_name", "users", "testdb", IndexType.BTree, ["name"], schema: schema);

        // Create an index on 'id' (unique)
        _indexManager.CreateIndex("idx_id", "users", "testdb", IndexType.BTree, ["id"], isUnique: true, schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("id = 5");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        // Should select idx_id for id column
        Assert.NotNull(result);
        Assert.Equal("idx_id", result!.IndexInfo.IndexName);
    }

    #endregion

    #region Range Query Tests

    [Fact]
    public void SelectIndex_SelectsBTreeIndex_ForRangeQuery()
    {
        var schema = SetupTestTable();

        _indexManager.CreateIndex("idx_age", "users", "testdb", IndexType.BTree, ["age"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("age > 18");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.NotNull(result);
        Assert.Equal("idx_age", result!.IndexInfo.IndexName);
        Assert.False(result.ScanRange.IsPointLookup);
    }

    [Fact]
    public void SelectIndex_SelectsBTreeIndex_ForBetween()
    {
        var schema = SetupTestTable();

        _indexManager.CreateIndex("idx_age", "users", "testdb", IndexType.BTree, ["age"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("age BETWEEN 18 AND 65");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.NotNull(result);
        Assert.Equal("idx_age", result!.IndexInfo.IndexName);
    }

    [Fact]
    public void SelectIndex_PrefersHashIndex_ForEquality()
    {
        var schema = SetupTestTable();

        // Create both B-Tree and Hash indexes on status
        _indexManager.CreateIndex("idx_status_btree", "users", "testdb", IndexType.BTree, ["status"], schema: schema);
        _indexManager.CreateIndex("idx_status_hash", "users", "testdb", IndexType.Hash, ["status"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("status = 'active'");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.NotNull(result);
        // Hash index should be preferred for equality
        Assert.Equal("idx_status_hash", result!.IndexInfo.IndexName);
    }

    [Fact]
    public void SelectIndex_PrefersBTreeIndex_ForRangeWithHashAvailable()
    {
        var schema = SetupTestTable();

        // Create both B-Tree and Hash indexes on age
        _indexManager.CreateIndex("idx_age_btree", "users", "testdb", IndexType.BTree, ["age"], schema: schema);
        _indexManager.CreateIndex("idx_age_hash", "users", "testdb", IndexType.Hash, ["age"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("age > 30");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.NotNull(result);
        // B-Tree should be preferred for range queries
        Assert.Equal("idx_age_btree", result!.IndexInfo.IndexName);
    }

    #endregion

    #region Composite Index Tests

    [Fact]
    public void SelectIndex_UsesCompositeIndex_WhenLeadingColumnsMatch()
    {
        var schema = SetupTestTable();

        // Create composite index on (name, age)
        _indexManager.CreateIndex("idx_name_age", "users", "testdb", IndexType.BTree, ["name", "age"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("name = 'John'");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.NotNull(result);
        Assert.Equal("idx_name_age", result!.IndexInfo.IndexName);
    }

    [Fact]
    public void SelectIndex_UsesFullCompositeIndex_WhenAllColumnsMatch()
    {
        var schema = SetupTestTable();

        // Create composite index on (name, age)
        _indexManager.CreateIndex("idx_name_age", "users", "testdb", IndexType.BTree, ["name", "age"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("name = 'John' AND age = 30");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.NotNull(result);
        Assert.Equal("idx_name_age", result!.IndexInfo.IndexName);
        Assert.Equal(2, result.MatchedPredicates.Count);
    }

    [Fact]
    public void SelectIndex_DoesNotUseCompositeIndex_WhenNonLeadingColumn()
    {
        var schema = SetupTestTable();

        // Create composite index on (name, age)
        _indexManager.CreateIndex("idx_name_age", "users", "testdb", IndexType.BTree, ["name", "age"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        // Only age is specified, but it's not the leading column
        var whereClause = ParseWhereClause("age = 30");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        // Index can't be used because leading column 'name' is not in predicate
        Assert.Null(result);
    }

    #endregion

    #region Predicate Extraction Tests

    [Fact]
    public void SelectIndex_ExtractsPredicatesFromAnd()
    {
        var schema = SetupTestTable();

        _indexManager.CreateIndex("idx_id", "users", "testdb", IndexType.BTree, ["id"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("id = 1 AND name = 'John'");

        var result = selector.SelectIndex("testdb", "users", whereClause);

        Assert.NotNull(result);
        // Should use idx_id for the id = 1 predicate
        Assert.Single(result!.MatchedPredicates);
        Assert.Equal("id", result.MatchedPredicates[0].ColumnName);
    }

    [Fact]
    public void SelectIndex_HandlesIsNull()
    {
        var schema = SetupTestTable();

        _indexManager.CreateIndex("idx_age", "users", "testdb", IndexType.BTree, ["age"], schema: schema);

        var selector = new IndexSelector(_catalog, _indexManager);
        var whereClause = ParseWhereClause("age IS NULL");

        // IS NULL predicates may or may not use index depending on implementation
        var result = selector.SelectIndex("testdb", "users", whereClause);

        // This might be null if IS NULL doesn't trigger index usage
        // Just verify it doesn't throw
    }

    #endregion

    #region IndexScanRange Tests

    [Fact]
    public void IndexScanRange_PointLookup_IsCorrect()
    {
        var key = new DataValue[] { DataValue.FromInt(5) };
        var range = IndexScanRange.PointLookup(key);

        Assert.True(range.IsPointLookup);
        Assert.NotNull(range.LowKey);
        Assert.NotNull(range.HighKey);
        Assert.True(range.LowInclusive);
        Assert.True(range.HighInclusive);
    }

    [Fact]
    public void IndexScanRange_Range_IsCorrect()
    {
        var low = new DataValue[] { DataValue.FromInt(5) };
        var high = new DataValue[] { DataValue.FromInt(10) };
        var range = IndexScanRange.Range(low, high, lowInclusive: true, highInclusive: false);

        Assert.False(range.IsPointLookup);
        Assert.NotNull(range.LowKey);
        Assert.NotNull(range.HighKey);
        Assert.True(range.LowInclusive);
        Assert.False(range.HighInclusive);
    }

    [Fact]
    public void IndexScanRange_FullScan_IsCorrect()
    {
        var range = IndexScanRange.FullScan();

        Assert.False(range.IsPointLookup);
        Assert.Null(range.LowKey);
        Assert.Null(range.HighKey);
    }

    #endregion
}
