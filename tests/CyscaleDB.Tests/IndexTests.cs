using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Index;

namespace CyscaleDB.Tests;

public class IndexTests : IDisposable
{
    private readonly string _testDir;

    public IndexTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"CyscaleDB_IndexTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    #region IndexInfo Tests

    [Fact]
    public void IndexInfo_Create_WithValidParameters()
    {
        var info = new IndexInfo(1, "idx_users_name", "users", "testdb",
            IndexType.BTree, ["name"], isUnique: false);

        Assert.Equal(1, info.IndexId);
        Assert.Equal("idx_users_name", info.IndexName);
        Assert.Equal("users", info.TableName);
        Assert.Equal("testdb", info.DatabaseName);
        Assert.Equal(IndexType.BTree, info.Type);
        Assert.Single(info.Columns);
        Assert.Equal("name", info.Columns[0]);
        Assert.False(info.IsUnique);
        Assert.False(info.IsPrimaryKey);
    }

    [Fact]
    public void IndexInfo_Create_MultiColumn()
    {
        var info = new IndexInfo(2, "idx_composite", "orders", "testdb",
            IndexType.BTree, ["customer_id", "order_date"], isUnique: true);

        Assert.Equal(2, info.Columns.Count);
        Assert.Equal("customer_id", info.Columns[0]);
        Assert.Equal("order_date", info.Columns[1]);
        Assert.True(info.IsUnique);
    }

    [Fact]
    public void IndexInfo_Create_PrimaryKey_ImpliesUnique()
    {
        var info = new IndexInfo(3, "pk_users", "users", "testdb",
            IndexType.BTree, ["id"], isUnique: false, isPrimaryKey: true);

        Assert.True(info.IsPrimaryKey);
        Assert.True(info.IsUnique); // Primary key implies unique
    }

    [Fact]
    public void IndexInfo_Serialize_Deserialize()
    {
        var original = new IndexInfo(5, "idx_test", "test_table", "test_db",
            IndexType.Hash, ["col1", "col2"], isUnique: true);

        var bytes = original.Serialize();
        var deserialized = IndexInfo.Deserialize(bytes);

        Assert.Equal(original.IndexId, deserialized.IndexId);
        Assert.Equal(original.IndexName, deserialized.IndexName);
        Assert.Equal(original.TableName, deserialized.TableName);
        Assert.Equal(original.DatabaseName, deserialized.DatabaseName);
        Assert.Equal(original.Type, deserialized.Type);
        Assert.Equal(original.Columns.Count, deserialized.Columns.Count);
        Assert.Equal(original.IsUnique, deserialized.IsUnique);
    }

    [Fact]
    public void IndexInfo_ExtractKeyValues()
    {
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, nullable: false, isPrimaryKey: true),
            new("name", DataType.VarChar, 100),
            new("age", DataType.Int)
        };
        var schema = new TableSchema(1, "testdb", "users", columns);

        var info = new IndexInfo(1, "idx_name", "users", "testdb",
            IndexType.BTree, ["name"]);
        info.ResolveColumnOrdinals(schema);

        var row = new Row(schema, [
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30)
        ]);

        var keys = info.ExtractKeyValues(row);

        Assert.Single(keys);
        Assert.Equal("John", keys[0].AsVarChar());
    }

    [Fact]
    public void IndexInfo_Create_ThrowsForEmptyName()
    {
        Assert.Throws<ArgumentException>(() =>
            new IndexInfo(1, "", "users", "testdb", IndexType.BTree, ["name"]));
    }

    [Fact]
    public void IndexInfo_Create_ThrowsForEmptyColumns()
    {
        Assert.Throws<ArgumentException>(() =>
            new IndexInfo(1, "idx_test", "users", "testdb", IndexType.BTree, []));
    }

    #endregion

    #region CompositeKey Tests

    [Fact]
    public void CompositeKey_Compare_Equal()
    {
        var key1 = new CompositeKey([DataValue.FromInt(1), DataValue.FromVarChar("test")]);
        var key2 = new CompositeKey([DataValue.FromInt(1), DataValue.FromVarChar("test")]);

        Assert.Equal(key1, key2);
        Assert.Equal(0, key1.CompareTo(key2));
    }

    [Fact]
    public void CompositeKey_Compare_LessThan()
    {
        var key1 = new CompositeKey([DataValue.FromInt(1)]);
        var key2 = new CompositeKey([DataValue.FromInt(2)]);

        Assert.True(key1.CompareTo(key2) < 0);
        Assert.True(key1 < key2);
    }

    [Fact]
    public void CompositeKey_Compare_GreaterThan()
    {
        var key1 = new CompositeKey([DataValue.FromInt(3)]);
        var key2 = new CompositeKey([DataValue.FromInt(2)]);

        Assert.True(key1.CompareTo(key2) > 0);
        Assert.True(key1 > key2);
    }

    [Fact]
    public void CompositeKey_Compare_MultiColumn()
    {
        var key1 = new CompositeKey([DataValue.FromInt(1), DataValue.FromVarChar("aaa")]);
        var key2 = new CompositeKey([DataValue.FromInt(1), DataValue.FromVarChar("bbb")]);

        Assert.True(key1.CompareTo(key2) < 0);
    }

    [Fact]
    public void CompositeKey_HashCode_SameForEqualKeys()
    {
        var key1 = new CompositeKey([DataValue.FromInt(1)]);
        var key2 = new CompositeKey([DataValue.FromInt(1)]);

        Assert.Equal(key1.GetHashCode(), key2.GetHashCode());
    }

    #endregion

    #region BTreeIndex Tests

    [Fact]
    public void BTreeIndex_InsertAndLookup_SingleKey()
    {
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, nullable: false, isPrimaryKey: true),
            new("name", DataType.VarChar, 100)
        };
        var schema = new TableSchema(1, "testdb", "users", columns);

        var indexInfo = new IndexInfo(1, "idx_id", "users", "testdb",
            IndexType.BTree, ["id"], isUnique: true);
        indexInfo.ResolveColumnOrdinals(schema);

        var filePath = Path.Combine(_testDir, "test.idx");
        using var pageManager = new PageManager(filePath);
        pageManager.Open(createIfNotExists: true);

        using var index = new BTreeIndex(indexInfo, pageManager);

        // Insert keys
        index.Insert([DataValue.FromInt(1)], new RowId(0, 0));
        index.Insert([DataValue.FromInt(2)], new RowId(0, 1));
        index.Insert([DataValue.FromInt(3)], new RowId(1, 0));

        // Lookup
        var result = index.Lookup([DataValue.FromInt(2)]).ToList();

        Assert.Single(result);
        Assert.Equal(new RowId(0, 1), result[0]);
    }

    [Fact]
    public void BTreeIndex_RangeScan()
    {
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, nullable: false),
            new("value", DataType.Int)
        };
        var schema = new TableSchema(1, "testdb", "test", columns);

        var indexInfo = new IndexInfo(1, "idx_id", "test", "testdb",
            IndexType.BTree, ["id"]);
        indexInfo.ResolveColumnOrdinals(schema);

        var filePath = Path.Combine(_testDir, "range.idx");
        using var pageManager = new PageManager(filePath);
        pageManager.Open(createIfNotExists: true);

        using var index = new BTreeIndex(indexInfo, pageManager);

        // Insert keys 1-10
        for (int i = 1; i <= 10; i++)
        {
            index.Insert([DataValue.FromInt(i)], new RowId(0, (short)i));
        }

        // Range scan 3-7
        var results = index.RangeScan(
            [DataValue.FromInt(3)],
            [DataValue.FromInt(7)]).ToList();

        Assert.Equal(5, results.Count);
    }

    [Fact]
    public void BTreeIndex_Delete()
    {
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, nullable: false)
        };
        var schema = new TableSchema(1, "testdb", "test", columns);

        var indexInfo = new IndexInfo(1, "idx_id", "test", "testdb",
            IndexType.BTree, ["id"]);
        indexInfo.ResolveColumnOrdinals(schema);

        var filePath = Path.Combine(_testDir, "delete.idx");
        using var pageManager = new PageManager(filePath);
        pageManager.Open(createIfNotExists: true);

        using var index = new BTreeIndex(indexInfo, pageManager);

        var rowId = new RowId(0, 0);
        index.Insert([DataValue.FromInt(5)], rowId);

        // Verify inserted
        var before = index.Lookup([DataValue.FromInt(5)]).ToList();
        Assert.Single(before);

        // Delete
        index.Delete([DataValue.FromInt(5)], rowId);

        // Verify deleted
        var after = index.Lookup([DataValue.FromInt(5)]).ToList();
        Assert.Empty(after);
    }

    [Fact]
    public void BTreeIndex_UniqueConstraint_ThrowsOnDuplicate()
    {
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, nullable: false)
        };
        var schema = new TableSchema(1, "testdb", "test", columns);

        var indexInfo = new IndexInfo(1, "idx_unique", "test", "testdb",
            IndexType.BTree, ["id"], isUnique: true);
        indexInfo.ResolveColumnOrdinals(schema);

        var filePath = Path.Combine(_testDir, "unique.idx");
        using var pageManager = new PageManager(filePath);
        pageManager.Open(createIfNotExists: true);

        using var index = new BTreeIndex(indexInfo, pageManager);

        index.Insert([DataValue.FromInt(1)], new RowId(0, 0));

        Assert.Throws<CyscaleException>(() =>
            index.Insert([DataValue.FromInt(1)], new RowId(0, 1)));
    }

    #endregion

    #region HashIndex Tests

    [Fact]
    public void HashIndex_InsertAndLookup()
    {
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, nullable: false)
        };
        var schema = new TableSchema(1, "testdb", "test", columns);

        var indexInfo = new IndexInfo(1, "idx_hash", "test", "testdb",
            IndexType.Hash, ["id"]);
        indexInfo.ResolveColumnOrdinals(schema);

        var filePath = Path.Combine(_testDir, "hash.hidx");
        using var pageManager = new PageManager(filePath);
        pageManager.Open(createIfNotExists: true);

        using var index = new HashIndex(indexInfo, pageManager);

        // Insert
        index.Insert([DataValue.FromInt(42)], new RowId(0, 0));
        index.Insert([DataValue.FromInt(100)], new RowId(0, 1));

        // Lookup
        var result = index.Lookup([DataValue.FromInt(42)]).ToList();

        Assert.Single(result);
        Assert.Equal(new RowId(0, 0), result[0]);
    }

    [Fact]
    public void HashIndex_Delete()
    {
        var columns = new List<ColumnDefinition>
        {
            new("id", DataType.Int, nullable: false)
        };
        var schema = new TableSchema(1, "testdb", "test", columns);

        var indexInfo = new IndexInfo(1, "idx_hash", "test", "testdb",
            IndexType.Hash, ["id"]);
        indexInfo.ResolveColumnOrdinals(schema);

        var filePath = Path.Combine(_testDir, "hash_delete.hidx");
        using var pageManager = new PageManager(filePath);
        pageManager.Open(createIfNotExists: true);

        using var index = new HashIndex(indexInfo, pageManager);

        var rowId = new RowId(0, 5);
        index.Insert([DataValue.FromInt(99)], rowId);

        index.Delete([DataValue.FromInt(99)], rowId);

        var result = index.Lookup([DataValue.FromInt(99)]).ToList();
        Assert.Empty(result);
    }

    #endregion
}
