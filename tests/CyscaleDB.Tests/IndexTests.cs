using NUnit.Framework;
using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;
using CyscaleDB.Core.Storage.Index;

namespace CyscaleDB.Tests;

[TestFixture]
public class IndexTests
{
    private string _testDir = null!;

    [SetUp]
    public void SetUp()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"CyscaleDB_IndexTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDir);
    }

    [TearDown]
    public void TearDown()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, true);
        }
    }

    #region IndexInfo Tests

    [Test]
    public void IndexInfo_Create_WithValidParameters()
    {
        var info = new IndexInfo(1, "idx_users_name", "users", "testdb",
            IndexType.BTree, ["name"], isUnique: false);

        Assert.Multiple(() =>
        {
            Assert.That(info.IndexId, Is.EqualTo(1));
            Assert.That(info.IndexName, Is.EqualTo("idx_users_name"));
            Assert.That(info.TableName, Is.EqualTo("users"));
            Assert.That(info.DatabaseName, Is.EqualTo("testdb"));
            Assert.That(info.Type, Is.EqualTo(IndexType.BTree));
            Assert.That(info.Columns, Has.Count.EqualTo(1));
            Assert.That(info.Columns[0], Is.EqualTo("name"));
            Assert.That(info.IsUnique, Is.False);
            Assert.That(info.IsPrimaryKey, Is.False);
        });
    }

    [Test]
    public void IndexInfo_Create_MultiColumn()
    {
        var info = new IndexInfo(2, "idx_composite", "orders", "testdb",
            IndexType.BTree, ["customer_id", "order_date"], isUnique: true);

        Assert.Multiple(() =>
        {
            Assert.That(info.Columns, Has.Count.EqualTo(2));
            Assert.That(info.Columns[0], Is.EqualTo("customer_id"));
            Assert.That(info.Columns[1], Is.EqualTo("order_date"));
            Assert.That(info.IsUnique, Is.True);
        });
    }

    [Test]
    public void IndexInfo_Create_PrimaryKey_ImpliesUnique()
    {
        var info = new IndexInfo(3, "pk_users", "users", "testdb",
            IndexType.BTree, ["id"], isUnique: false, isPrimaryKey: true);

        Assert.Multiple(() =>
        {
            Assert.That(info.IsPrimaryKey, Is.True);
            Assert.That(info.IsUnique, Is.True); // Primary key implies unique
        });
    }

    [Test]
    public void IndexInfo_Serialize_Deserialize()
    {
        var original = new IndexInfo(5, "idx_test", "test_table", "test_db",
            IndexType.Hash, ["col1", "col2"], isUnique: true);

        var bytes = original.Serialize();
        var deserialized = IndexInfo.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.IndexId, Is.EqualTo(original.IndexId));
            Assert.That(deserialized.IndexName, Is.EqualTo(original.IndexName));
            Assert.That(deserialized.TableName, Is.EqualTo(original.TableName));
            Assert.That(deserialized.DatabaseName, Is.EqualTo(original.DatabaseName));
            Assert.That(deserialized.Type, Is.EqualTo(original.Type));
            Assert.That(deserialized.Columns.Count, Is.EqualTo(original.Columns.Count));
            Assert.That(deserialized.IsUnique, Is.EqualTo(original.IsUnique));
        });
    }

    [Test]
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

        Assert.That(keys.Length, Is.EqualTo(1));
        Assert.That(keys[0].AsVarChar(), Is.EqualTo("John"));
    }

    [Test]
    public void IndexInfo_Create_ThrowsForEmptyName()
    {
        Assert.Throws<ArgumentException>(() =>
            new IndexInfo(1, "", "users", "testdb", IndexType.BTree, ["name"]));
    }

    [Test]
    public void IndexInfo_Create_ThrowsForEmptyColumns()
    {
        Assert.Throws<ArgumentException>(() =>
            new IndexInfo(1, "idx_test", "users", "testdb", IndexType.BTree, []));
    }

    #endregion

    #region CompositeKey Tests

    [Test]
    public void CompositeKey_Compare_Equal()
    {
        var key1 = new CompositeKey([DataValue.FromInt(1), DataValue.FromVarChar("test")]);
        var key2 = new CompositeKey([DataValue.FromInt(1), DataValue.FromVarChar("test")]);

        Assert.That(key1, Is.EqualTo(key2));
        Assert.That(key1.CompareTo(key2), Is.EqualTo(0));
    }

    [Test]
    public void CompositeKey_Compare_LessThan()
    {
        var key1 = new CompositeKey([DataValue.FromInt(1)]);
        var key2 = new CompositeKey([DataValue.FromInt(2)]);

        Assert.That(key1.CompareTo(key2), Is.LessThan(0));
        Assert.That(key1 < key2, Is.True);
    }

    [Test]
    public void CompositeKey_Compare_GreaterThan()
    {
        var key1 = new CompositeKey([DataValue.FromInt(3)]);
        var key2 = new CompositeKey([DataValue.FromInt(2)]);

        Assert.That(key1.CompareTo(key2), Is.GreaterThan(0));
        Assert.That(key1 > key2, Is.True);
    }

    [Test]
    public void CompositeKey_Compare_MultiColumn()
    {
        var key1 = new CompositeKey([DataValue.FromInt(1), DataValue.FromVarChar("aaa")]);
        var key2 = new CompositeKey([DataValue.FromInt(1), DataValue.FromVarChar("bbb")]);

        Assert.That(key1.CompareTo(key2), Is.LessThan(0));
    }

    [Test]
    public void CompositeKey_HashCode_SameForEqualKeys()
    {
        var key1 = new CompositeKey([DataValue.FromInt(1)]);
        var key2 = new CompositeKey([DataValue.FromInt(1)]);

        Assert.That(key1.GetHashCode(), Is.EqualTo(key2.GetHashCode()));
    }

    #endregion

    #region BTreeIndex Tests

    [Test]
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

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0], Is.EqualTo(new RowId(0, 1)));
    }

    [Test]
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

        Assert.That(results, Has.Count.EqualTo(5));
    }

    [Test]
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
        Assert.That(before, Has.Count.EqualTo(1));

        // Delete
        index.Delete([DataValue.FromInt(5)], rowId);

        // Verify deleted
        var after = index.Lookup([DataValue.FromInt(5)]).ToList();
        Assert.That(after, Has.Count.EqualTo(0));
    }

    [Test]
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

    [Test]
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

        Assert.That(result, Has.Count.EqualTo(1));
        Assert.That(result[0], Is.EqualTo(new RowId(0, 0)));
    }

    [Test]
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
        Assert.That(result, Has.Count.EqualTo(0));
    }

    #endregion
}
