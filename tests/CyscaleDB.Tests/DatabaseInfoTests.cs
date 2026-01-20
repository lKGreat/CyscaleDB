using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class DatabaseInfoTests
{
    private static TableSchema CreateTestTable(int tableId, string tableName)
    {
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 100)
        };
        return new TableSchema(tableId, "test_db", tableName, columns);
    }

    [Fact]
    public void Constructor_ValidParameters_CreatesDatabase()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");

        Assert.Equal(1, db.DatabaseId);
        Assert.Equal("test_db", db.Name);
        Assert.Equal("/data/test_db", db.DataDirectory);
        Assert.Equal(0, db.TableCount);
        Assert.Equal("utf8mb4", db.CharacterSet);
    }

    [Fact]
    public void Constructor_EmptyName_Throws()
    {
        Assert.Throws<ArgumentException>(() => new DatabaseInfo(1, "", "/data"));
    }

    [Fact]
    public void Constructor_TooLongName_Throws()
    {
        var longName = new string('x', Constants.MaxDatabaseNameLength + 1);
        Assert.Throws<ArgumentException>(() => new DatabaseInfo(1, longName, "/data"));
    }

    [Fact]
    public void AddTable_ValidTable_AddsToDatabase()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");
        var table = CreateTestTable(1, "users");

        db.AddTable(table);

        Assert.Equal(1, db.TableCount);
        Assert.True(db.HasTable("users"));
    }

    [Fact]
    public void AddTable_DuplicateName_Throws()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");
        var table1 = CreateTestTable(1, "users");
        var table2 = CreateTestTable(2, "users");

        db.AddTable(table1);

        Assert.Throws<TableExistsException>(() => db.AddTable(table2));
    }

    [Fact]
    public void GetTable_ExistingTable_ReturnsTable()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");
        var table = CreateTestTable(1, "users");
        db.AddTable(table);

        var result = db.GetTable("users");

        Assert.NotNull(result);
        Assert.Equal("users", result.TableName);
    }

    [Fact]
    public void GetTable_CaseInsensitive_ReturnsTable()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");
        var table = CreateTestTable(1, "Users");
        db.AddTable(table);

        var result = db.GetTable("USERS");

        Assert.NotNull(result);
    }

    [Fact]
    public void GetTable_NonExisting_ReturnsNull()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");

        var result = db.GetTable("nonexistent");

        Assert.Null(result);
    }

    [Fact]
    public void RemoveTable_ExistingTable_RemovesAndReturnsTrue()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");
        var table = CreateTestTable(1, "users");
        db.AddTable(table);

        var result = db.RemoveTable("users");

        Assert.True(result);
        Assert.False(db.HasTable("users"));
        Assert.Equal(0, db.TableCount);
    }

    [Fact]
    public void RemoveTable_NonExisting_ReturnsFalse()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");

        var result = db.RemoveTable("nonexistent");

        Assert.False(result);
    }

    [Fact]
    public void GetNextTableId_Increments()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");

        Assert.Equal(1, db.GetNextTableId());
        Assert.Equal(2, db.GetNextTableId());
        Assert.Equal(3, db.GetNextTableId());
    }

    [Fact]
    public void Tables_ReturnsAllTables()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");
        db.AddTable(CreateTestTable(1, "users"));
        db.AddTable(CreateTestTable(2, "orders"));
        db.AddTable(CreateTestTable(3, "products"));

        var tables = db.Tables.ToList();

        Assert.Equal(3, tables.Count);
        Assert.Contains(tables, t => t.TableName == "users");
        Assert.Contains(tables, t => t.TableName == "orders");
        Assert.Contains(tables, t => t.TableName == "products");
    }

    [Fact]
    public void Serialize_EmptyDatabase_RoundTrips()
    {
        var original = new DatabaseInfo(1, "test_db", "/data/test_db");

        var bytes = original.Serialize();
        var restored = DatabaseInfo.Deserialize(bytes);

        Assert.Equal(original.DatabaseId, restored.DatabaseId);
        Assert.Equal(original.Name, restored.Name);
        Assert.Equal(original.DataDirectory, restored.DataDirectory);
        Assert.Equal(original.CharacterSet, restored.CharacterSet);
        Assert.Equal(original.Collation, restored.Collation);
        Assert.Equal(0, restored.TableCount);
    }

    [Fact]
    public void Serialize_WithTables_RoundTrips()
    {
        var original = new DatabaseInfo(1, "test_db", "/data/test_db");
        original.AddTable(CreateTestTable(1, "users"));
        original.AddTable(CreateTestTable(2, "orders"));

        // Consume some table IDs
        original.GetNextTableId();
        original.GetNextTableId();

        var bytes = original.Serialize();
        var restored = DatabaseInfo.Deserialize(bytes);

        Assert.Equal(2, restored.TableCount);
        Assert.True(restored.HasTable("users"));
        Assert.True(restored.HasTable("orders"));

        // Next table ID should be preserved
        var nextId = restored.GetNextTableId();
        Assert.True(nextId > 2);
    }

    [Fact]
    public void ToString_FormatsCorrectly()
    {
        var db = new DatabaseInfo(1, "test_db", "/data/test_db");
        db.AddTable(CreateTestTable(1, "users"));
        db.AddTable(CreateTestTable(2, "orders"));

        var str = db.ToString();

        Assert.Contains("test_db", str);
        Assert.Contains("2 tables", str);
    }
}
