using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class TableSchemaTests
{
    private static ColumnDefinition CreateColumn(string name, DataType type, bool isPrimaryKey = false, bool isNullable = true)
    {
        return new ColumnDefinition(name, type, isPrimaryKey: isPrimaryKey, isNullable: isNullable);
    }

    [Fact]
    public void Constructor_ValidParameters_CreatesSchema()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int, isPrimaryKey: true),
            CreateColumn("name", DataType.VarChar),
            CreateColumn("age", DataType.Int)
        };

        var schema = new TableSchema(1, "test_db", "users", columns);

        Assert.Equal(1, schema.TableId);
        Assert.Equal("test_db", schema.DatabaseName);
        Assert.Equal("users", schema.TableName);
        Assert.Equal("test_db.users", schema.FullName);
        Assert.Equal(3, schema.Columns.Count);
    }

    [Fact]
    public void Constructor_EmptyColumns_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            new TableSchema(1, "test_db", "users", Array.Empty<ColumnDefinition>()));
    }

    [Fact]
    public void Constructor_DuplicateColumnNames_Throws()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int),
            CreateColumn("ID", DataType.BigInt) // Duplicate (case-insensitive)
        };

        Assert.Throws<ArgumentException>(() =>
            new TableSchema(1, "test_db", "users", columns));
    }

    [Fact]
    public void GetColumn_ByName_ReturnsColumn()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int),
            CreateColumn("name", DataType.VarChar)
        };
        var schema = new TableSchema(1, "test_db", "users", columns);

        var column = schema.GetColumn("name");

        Assert.NotNull(column);
        Assert.Equal("name", column.Name);
        Assert.Equal(DataType.VarChar, column.DataType);
    }

    [Fact]
    public void GetColumn_CaseInsensitive_ReturnsColumn()
    {
        var columns = new[] { CreateColumn("Name", DataType.VarChar) };
        var schema = new TableSchema(1, "test_db", "users", columns);

        var column = schema.GetColumn("NAME");

        Assert.NotNull(column);
        Assert.Equal("Name", column.Name);
    }

    [Fact]
    public void GetColumn_NotFound_ReturnsNull()
    {
        var columns = new[] { CreateColumn("id", DataType.Int) };
        var schema = new TableSchema(1, "test_db", "users", columns);

        var column = schema.GetColumn("nonexistent");

        Assert.Null(column);
    }

    [Fact]
    public void GetColumnOrdinal_ReturnsCorrectPosition()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int),
            CreateColumn("name", DataType.VarChar),
            CreateColumn("age", DataType.Int)
        };
        var schema = new TableSchema(1, "test_db", "users", columns);

        Assert.Equal(0, schema.GetColumnOrdinal("id"));
        Assert.Equal(1, schema.GetColumnOrdinal("name"));
        Assert.Equal(2, schema.GetColumnOrdinal("age"));
        Assert.Equal(-1, schema.GetColumnOrdinal("nonexistent"));
    }

    [Fact]
    public void PrimaryKeyColumns_ReturnsOnlyPrimaryKeys()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int, isPrimaryKey: true),
            CreateColumn("name", DataType.VarChar),
            CreateColumn("tenant_id", DataType.Int, isPrimaryKey: true)
        };
        var schema = new TableSchema(1, "test_db", "users", columns);

        Assert.Equal(2, schema.PrimaryKeyColumns.Count);
        Assert.Contains(schema.PrimaryKeyColumns, c => c.Name == "id");
        Assert.Contains(schema.PrimaryKeyColumns, c => c.Name == "tenant_id");
    }

    [Fact]
    public void AutoIncrement_GetNextValue_Increments()
    {
        var columns = new[] { CreateColumn("id", DataType.Int) };
        var schema = new TableSchema(1, "test_db", "users", columns);

        Assert.Equal(1, schema.GetNextAutoIncrementValue());
        Assert.Equal(2, schema.GetNextAutoIncrementValue());
        Assert.Equal(3, schema.GetNextAutoIncrementValue());
    }

    [Fact]
    public void AutoIncrement_UpdateValue_SetsHigherValue()
    {
        var columns = new[] { CreateColumn("id", DataType.Int) };
        var schema = new TableSchema(1, "test_db", "users", columns);

        schema.UpdateAutoIncrementValue(100);

        Assert.Equal(101, schema.GetNextAutoIncrementValue());
    }

    [Fact]
    public void Serialize_RoundTrips()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int, isPrimaryKey: true),
            CreateColumn("name", DataType.VarChar),
            CreateColumn("created_at", DataType.DateTime)
        };
        var original = new TableSchema(1, "test_db", "users", columns);
        original.UpdateRowCount(100);
        original.UpdateAutoIncrementValue(50);

        var bytes = original.Serialize();
        var restored = TableSchema.Deserialize(bytes);

        Assert.Equal(original.TableId, restored.TableId);
        Assert.Equal(original.DatabaseName, restored.DatabaseName);
        Assert.Equal(original.TableName, restored.TableName);
        Assert.Equal(original.Columns.Count, restored.Columns.Count);
        Assert.Equal(original.RowCount, restored.RowCount);
        Assert.Equal(51, restored.GetNextAutoIncrementValue()); // 50 + 1
    }

    [Fact]
    public void ValidateRow_ValidRow_Succeeds()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int),
            CreateColumn("name", DataType.VarChar)
        };
        var schema = new TableSchema(1, "test_db", "users", columns);
        var values = new[] { DataValue.FromInt(1), DataValue.FromVarChar("John") };

        // Should not throw
        schema.ValidateRow(values);
    }

    [Fact]
    public void ValidateRow_WrongColumnCount_Throws()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int),
            CreateColumn("name", DataType.VarChar)
        };
        var schema = new TableSchema(1, "test_db", "users", columns);
        var values = new[] { DataValue.FromInt(1) }; // Missing one column

        Assert.Throws<ArgumentException>(() => schema.ValidateRow(values));
    }

    [Fact]
    public void ValidateRow_NullInNonNullableColumn_Throws()
    {
        var columns = new[]
        {
            CreateColumn("id", DataType.Int, isNullable: false)
        };
        var schema = new TableSchema(1, "test_db", "users", columns);
        var values = new[] { DataValue.Null };

        Assert.Throws<ArgumentException>(() => schema.ValidateRow(values));
    }
}
