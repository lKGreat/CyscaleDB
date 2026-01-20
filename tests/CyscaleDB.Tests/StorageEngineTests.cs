using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class StorageEngineTests : IDisposable
{
    private readonly string _testDir;
    private readonly StorageEngine _engine;

    public StorageEngineTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "CyscaleDB_Tests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDir);
        _engine = new StorageEngine(_testDir);
    }

    public void Dispose()
    {
        _engine.Dispose();
        
        if (Directory.Exists(_testDir))
        {
            try
            {
                Directory.Delete(_testDir, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Fact]
    public void CreateDatabase_ShouldSucceed()
    {
        var db = _engine.CreateDatabase("testdb");

        Assert.NotNull(db);
        Assert.Equal("testdb", db.Name);
        Assert.True(_engine.Catalog.DatabaseExists("testdb"));
    }

    [Fact]
    public void CreateDatabase_Duplicate_ShouldThrow()
    {
        _engine.CreateDatabase("testdb");

        Assert.Throws<DatabaseExistsException>(() => _engine.CreateDatabase("testdb"));
    }

    [Fact]
    public void DropDatabase_ShouldSucceed()
    {
        _engine.CreateDatabase("testdb");
        
        var result = _engine.DropDatabase("testdb");

        Assert.True(result);
        Assert.False(_engine.Catalog.DatabaseExists("testdb"));
    }

    [Fact]
    public void CreateTable_ShouldSucceed()
    {
        _engine.CreateDatabase("testdb");
        
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true, isAutoIncrement: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 100),
            new ColumnDefinition("age", DataType.Int)
        };

        var schema = _engine.CreateTable("testdb", "users", columns);

        Assert.NotNull(schema);
        Assert.Equal("users", schema.TableName);
        Assert.Equal(3, schema.Columns.Count);
    }

    [Fact]
    public void InsertAndReadRow_ShouldSucceed()
    {
        _engine.CreateDatabase("testdb");
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true, isAutoIncrement: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 100),
            new ColumnDefinition("age", DataType.Int)
        };
        _engine.CreateTable("testdb", "users", columns);

        var values = new DataValue[]
        {
            DataValue.Null, // Auto-increment
            DataValue.FromVarChar("Alice"),
            DataValue.FromInt(25)
        };

        var rowId = _engine.InsertRow("testdb", "users", values);
        var row = _engine.GetRow("testdb", "users", rowId);

        Assert.NotNull(row);
        Assert.Equal(1L, row.Values[0].AsBigInt()); // Auto-incremented
        Assert.Equal("Alice", row.Values[1].AsString());
        Assert.Equal(25, row.Values[2].AsInt());
    }

    [Fact]
    public void ScanTable_ShouldReturnAllRows()
    {
        _engine.CreateDatabase("testdb");
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int),
            new ColumnDefinition("value", DataType.VarChar, maxLength: 50)
        };
        _engine.CreateTable("testdb", "items", columns);

        for (int i = 0; i < 10; i++)
        {
            var values = new DataValue[]
            {
                DataValue.FromInt(i),
                DataValue.FromVarChar($"Item {i}")
            };
            _engine.InsertRow("testdb", "items", values);
        }

        var rows = _engine.ScanTable("testdb", "items").ToList();

        Assert.Equal(10, rows.Count);
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(i, rows[i].Values[0].AsInt());
            Assert.Equal($"Item {i}", rows[i].Values[1].AsString());
        }
    }

    [Fact]
    public void UpdateRow_ShouldModifyData()
    {
        _engine.CreateDatabase("testdb");
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 50)
        };
        var schema = _engine.CreateTable("testdb", "items", columns);

        var rowId = _engine.InsertRow("testdb", "items", new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Original")
        });

        var table = _engine.OpenTable("testdb", "items");
        var updatedRow = new Row(schema, new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Updated")
        });

        var result = _engine.UpdateRow("testdb", "items", rowId, updatedRow);
        var retrieved = _engine.GetRow("testdb", "items", rowId);

        Assert.True(result);
        Assert.Equal("Updated", retrieved!.Values[1].AsString());
    }

    [Fact]
    public void DeleteRow_ShouldRemoveData()
    {
        _engine.CreateDatabase("testdb");
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 50)
        };
        _engine.CreateTable("testdb", "items", columns);

        var rowId = _engine.InsertRow("testdb", "items", new DataValue[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Test")
        });

        var result = _engine.DeleteRow("testdb", "items", rowId);
        var retrieved = _engine.GetRow("testdb", "items", rowId);

        Assert.True(result);
        Assert.Null(retrieved);
    }

    [Fact]
    public void Persistence_DataShouldSurviveRestart()
    {
        // Insert data
        _engine.CreateDatabase("testdb");
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 50)
        };
        _engine.CreateTable("testdb", "items", columns);
        
        _engine.InsertRow("testdb", "items", new DataValue[]
        {
            DataValue.FromInt(42),
            DataValue.FromVarChar("Persistent Data")
        });
        
        _engine.Flush();
        _engine.Dispose();

        // Create new engine instance
        using var engine2 = new StorageEngine(_testDir);
        
        Assert.True(engine2.Catalog.DatabaseExists("testdb"));
        
        var rows = engine2.ScanTable("testdb", "items").ToList();
        Assert.Single(rows);
        Assert.Equal(42, rows[0].Values[0].AsInt());
        Assert.Equal("Persistent Data", rows[0].Values[1].AsString());
    }

    [Fact]
    public void BufferPool_ShouldCachePages()
    {
        _engine.CreateDatabase("testdb");
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int),
            new ColumnDefinition("data", DataType.VarChar, maxLength: 100)
        };
        _engine.CreateTable("testdb", "items", columns);

        // Insert some data to cause page reads/writes
        for (int i = 0; i < 100; i++)
        {
            _engine.InsertRow("testdb", "items", new DataValue[]
            {
                DataValue.FromInt(i),
                DataValue.FromVarChar($"Data for item {i}")
            });
        }

        var (cachedPages, capacity, hitRatio) = _engine.GetBufferPoolStats();
        
        Assert.True(cachedPages > 0);
        Assert.Equal(Constants.DefaultBufferPoolSize, capacity);
    }

    [Fact]
    public void DropTable_ShouldDeleteDataFile()
    {
        _engine.CreateDatabase("testdb");
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int)
        };
        _engine.CreateTable("testdb", "items", columns);
        _engine.InsertRow("testdb", "items", new DataValue[] { DataValue.FromInt(1) });
        
        var db = _engine.GetDatabase("testdb")!;
        var dataFilePath = Path.Combine(db.DataDirectory, "items.cdb");
        
        // File should exist before drop
        _engine.Flush();
        Assert.True(File.Exists(dataFilePath));

        _engine.DropTable("testdb", "items");

        // File should be deleted after drop
        Assert.False(File.Exists(dataFilePath));
    }

    [Fact]
    public void MultipleDataTypes_ShouldWorkCorrectly()
    {
        _engine.CreateDatabase("testdb");
        var columns = new[]
        {
            new ColumnDefinition("int_col", DataType.Int),
            new ColumnDefinition("bigint_col", DataType.BigInt),
            new ColumnDefinition("varchar_col", DataType.VarChar, maxLength: 200),
            new ColumnDefinition("bool_col", DataType.Boolean),
            new ColumnDefinition("float_col", DataType.Float),
            new ColumnDefinition("double_col", DataType.Double),
            new ColumnDefinition("datetime_col", DataType.DateTime)
        };
        _engine.CreateTable("testdb", "mixed_types", columns);

        var now = DateTime.UtcNow;
        var values = new DataValue[]
        {
            DataValue.FromInt(42),
            DataValue.FromBigInt(9999999999L),
            DataValue.FromVarChar("Hello, World!"),
            DataValue.FromBoolean(true),
            DataValue.FromFloat(3.14f),
            DataValue.FromDouble(2.71828),
            DataValue.FromDateTime(now)
        };

        var rowId = _engine.InsertRow("testdb", "mixed_types", values);
        var row = _engine.GetRow("testdb", "mixed_types", rowId);

        Assert.NotNull(row);
        Assert.Equal(42, row.Values[0].AsInt());
        Assert.Equal(9999999999L, row.Values[1].AsBigInt());
        Assert.Equal("Hello, World!", row.Values[2].AsString());
        Assert.True(row.Values[3].AsBoolean());
        Assert.Equal(3.14f, row.Values[4].AsFloat(), 0.001f);
        Assert.Equal(2.71828, row.Values[5].AsDouble(), 0.00001);
        Assert.Equal(now.Ticks, row.Values[6].AsDateTime().Ticks);
    }
}
