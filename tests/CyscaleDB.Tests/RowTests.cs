using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class RowTests
{
    private static TableSchema CreateTestSchema()
    {
        var columns = new[]
        {
            new ColumnDefinition("id", DataType.Int, isPrimaryKey: true),
            new ColumnDefinition("name", DataType.VarChar, maxLength: 100),
            new ColumnDefinition("age", DataType.Int, isNullable: true),
            new ColumnDefinition("active", DataType.Boolean),
            new ColumnDefinition("salary", DataType.Decimal, isNullable: true)
        };
        return new TableSchema(1, "test_db", "employees", columns);
    }

    [Fact]
    public void Constructor_ValidValues_CreatesRow()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John Doe"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(50000.00m)
        };

        var row = new Row(schema, values);

        Assert.Equal(schema, row.Schema);
        Assert.Equal(5, row.Values.Length);
    }

    [Fact]
    public void Constructor_WrongValueCount_Throws()
    {
        var schema = CreateTestSchema();
        var values = new[] { DataValue.FromInt(1), DataValue.FromVarChar("John") };

        Assert.Throws<ArgumentException>(() => new Row(schema, values));
    }

    [Fact]
    public void GetValue_ByName_ReturnsCorrectValue()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John Doe"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(50000.00m)
        };
        var row = new Row(schema, values);

        var nameValue = row.GetValue("name");

        Assert.Equal("John Doe", nameValue.AsString());
    }

    [Fact]
    public void GetValue_ByOrdinal_ReturnsCorrectValue()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John Doe"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(50000.00m)
        };
        var row = new Row(schema, values);

        var idValue = row.GetValue(0);
        var nameValue = row.GetValue(1);

        Assert.Equal(1, idValue.AsInt());
        Assert.Equal("John Doe", nameValue.AsString());
    }

    [Fact]
    public void GetValue_InvalidColumnName_Throws()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.Null,
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var row = new Row(schema, values);

        Assert.Throws<ArgumentException>(() => row.GetValue("nonexistent"));
    }

    [Fact]
    public void SetValue_ByOrdinal_UpdatesValue()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var row = new Row(schema, values);

        row.SetValue(1, DataValue.FromVarChar("Jane"));

        Assert.Equal("Jane", row.GetValue("name").AsString());
    }

    [Fact]
    public void SetValue_ByName_UpdatesValue()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var row = new Row(schema, values);

        row.SetValue("age", DataValue.FromInt(31));

        Assert.Equal(31, row.GetValue("age").AsInt());
    }

    [Fact]
    public void Serialize_WithNulls_RoundTrips()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("John"),
            DataValue.Null, // age is null
            DataValue.FromBoolean(false),
            DataValue.Null  // salary is null
        };
        var original = new Row(schema, values);

        var bytes = original.Serialize();
        var restored = Row.Deserialize(bytes, schema);

        Assert.Equal(original.Values.Length, restored.Values.Length);
        Assert.Equal(1, restored.GetValue("id").AsInt());
        Assert.Equal("John", restored.GetValue("name").AsString());
        Assert.True(restored.GetValue("age").IsNull);
        Assert.False(restored.GetValue("active").AsBoolean());
        Assert.True(restored.GetValue("salary").IsNull);
    }

    [Fact]
    public void Serialize_AllValueTypes_RoundTrips()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(42),
            DataValue.FromVarChar("Test String"),
            DataValue.FromInt(25),
            DataValue.FromBoolean(true),
            DataValue.FromDecimal(12345.67m)
        };
        var original = new Row(schema, values);

        var bytes = original.Serialize();
        var restored = Row.Deserialize(bytes, schema);

        Assert.Equal(42, restored.GetValue(0).AsInt());
        Assert.Equal("Test String", restored.GetValue(1).AsString());
        Assert.Equal(25, restored.GetValue(2).AsInt());
        Assert.True(restored.GetValue(3).AsBoolean());
        Assert.Equal(12345.67m, restored.GetValue(4).AsDecimal());
    }

    [Fact]
    public void Clone_CreatesIndependentCopy()
    {
        var schema = CreateTestSchema();
        var values = new[]
        {
            DataValue.FromInt(1),
            DataValue.FromVarChar("Original"),
            DataValue.FromInt(30),
            DataValue.FromBoolean(true),
            DataValue.Null
        };
        var original = new Row(schema, values);
        original.RowId = new RowId(1, 0);

        var clone = original.Clone();

        // Clone should have same values
        Assert.Equal(original.GetValue("name").AsString(), clone.GetValue("name").AsString());
        Assert.Equal(original.RowId, clone.RowId);

        // Modifying clone should not affect original
        clone.SetValue("name", DataValue.FromVarChar("Modified"));
        Assert.Equal("Original", original.GetValue("name").AsString());
        Assert.Equal("Modified", clone.GetValue("name").AsString());
    }

    [Fact]
    public void RowId_InvalidStaticProperty_IsNotValid()
    {
        var rowId = RowId.Invalid;
        Assert.False(rowId.IsValid);
        Assert.Equal(-1, rowId.PageId);
        Assert.Equal(-1, rowId.SlotNumber);
    }

    [Fact]
    public void RowId_ValidValues_IsValid()
    {
        var rowId = new RowId(1, 5);
        Assert.True(rowId.IsValid);
        Assert.Equal(1, rowId.PageId);
        Assert.Equal(5, rowId.SlotNumber);
    }

    [Fact]
    public void RowId_Equality_Works()
    {
        var a = new RowId(1, 5);
        var b = new RowId(1, 5);
        var c = new RowId(1, 6);

        Assert.True(a == b);
        Assert.False(a == c);
        Assert.True(a != c);
    }
}
