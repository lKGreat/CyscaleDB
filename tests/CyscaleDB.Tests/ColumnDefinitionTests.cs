using CyscaleDB.Core.Common;
using CyscaleDB.Core.Storage;

namespace CyscaleDB.Tests;

public class ColumnDefinitionTests
{
    [Fact]
    public void Constructor_ValidParameters_CreatesColumn()
    {
        var column = new ColumnDefinition("id", DataType.Int);

        Assert.Equal("id", column.Name);
        Assert.Equal(DataType.Int, column.DataType);
        Assert.True(column.IsNullable);
        Assert.False(column.IsPrimaryKey);
        Assert.False(column.IsAutoIncrement);
    }

    [Fact]
    public void Constructor_PrimaryKey_SetsNotNullable()
    {
        var column = new ColumnDefinition("id", DataType.Int, isPrimaryKey: true, isNullable: true);

        Assert.True(column.IsPrimaryKey);
        Assert.False(column.IsNullable); // Primary keys are never nullable
    }

    [Fact]
    public void Constructor_VarChar_SetsDefaultMaxLength()
    {
        var column = new ColumnDefinition("name", DataType.VarChar);

        Assert.Equal(Constants.DefaultVarCharLength, column.MaxLength);
    }

    [Fact]
    public void Constructor_VarCharWithLength_SetsSpecifiedLength()
    {
        var column = new ColumnDefinition("name", DataType.VarChar, maxLength: 100);

        Assert.Equal(100, column.MaxLength);
    }

    [Fact]
    public void Constructor_EmptyName_Throws()
    {
        Assert.Throws<ArgumentException>(() => new ColumnDefinition("", DataType.Int));
    }

    [Fact]
    public void Constructor_TooLongName_Throws()
    {
        var longName = new string('x', Constants.MaxColumnNameLength + 1);
        Assert.Throws<ArgumentException>(() => new ColumnDefinition(longName, DataType.Int));
    }

    [Fact]
    public void Constructor_WithDefaultValue_SetsDefaultValue()
    {
        var defaultValue = DataValue.FromInt(0);
        var column = new ColumnDefinition("status", DataType.Int, defaultValue: defaultValue);

        Assert.NotNull(column.DefaultValue);
        Assert.Equal(0, column.DefaultValue.Value.AsInt());
    }

    [Fact]
    public void GetByteSize_FixedTypes_ReturnsCorrectSize()
    {
        Assert.Equal(4, new ColumnDefinition("c", DataType.Int).GetByteSize());
        Assert.Equal(8, new ColumnDefinition("c", DataType.BigInt).GetByteSize());
        Assert.Equal(1, new ColumnDefinition("c", DataType.TinyInt).GetByteSize());
        Assert.Equal(8, new ColumnDefinition("c", DataType.Double).GetByteSize());
    }

    [Fact]
    public void ValidateValue_NullOnNullable_ReturnsTrue()
    {
        var column = new ColumnDefinition("name", DataType.VarChar, isNullable: true);

        Assert.True(column.ValidateValue(DataValue.Null));
    }

    [Fact]
    public void ValidateValue_NullOnNotNullable_ReturnsFalse()
    {
        var column = new ColumnDefinition("name", DataType.VarChar, isNullable: false);

        Assert.False(column.ValidateValue(DataValue.Null));
    }

    [Fact]
    public void ValidateValue_MatchingType_ReturnsTrue()
    {
        var column = new ColumnDefinition("id", DataType.Int);

        Assert.True(column.ValidateValue(DataValue.FromInt(42)));
    }

    [Fact]
    public void ValidateValue_StringTooLong_ReturnsFalse()
    {
        var column = new ColumnDefinition("code", DataType.VarChar, maxLength: 5);

        Assert.True(column.ValidateValue(DataValue.FromVarChar("ABC")));
        Assert.False(column.ValidateValue(DataValue.FromVarChar("ABCDEFGH")));
    }

    [Fact]
    public void ValidateValue_CompatibleNumericTypes_ReturnsTrue()
    {
        var intColumn = new ColumnDefinition("num", DataType.Int);
        var bigIntColumn = new ColumnDefinition("num", DataType.BigInt);

        // BigInt value should be compatible with Int column (type system allows promotion)
        Assert.True(intColumn.ValidateValue(DataValue.FromBigInt(42)));
        Assert.True(bigIntColumn.ValidateValue(DataValue.FromInt(42)));
    }

    [Fact]
    public void Serialize_RoundTrips()
    {
        var original = new ColumnDefinition(
            "test_column",
            DataType.VarChar,
            maxLength: 100,
            isNullable: false,
            isPrimaryKey: false,
            isAutoIncrement: false,
            defaultValue: DataValue.FromVarChar("default"));

        var bytes = original.Serialize();
        var restored = ColumnDefinition.Deserialize(bytes);

        Assert.Equal(original.Name, restored.Name);
        Assert.Equal(original.DataType, restored.DataType);
        Assert.Equal(original.MaxLength, restored.MaxLength);
        Assert.Equal(original.IsNullable, restored.IsNullable);
        Assert.Equal(original.IsPrimaryKey, restored.IsPrimaryKey);
        Assert.Equal(original.IsAutoIncrement, restored.IsAutoIncrement);
        Assert.NotNull(restored.DefaultValue);
        Assert.Equal("default", restored.DefaultValue.Value.AsString());
    }

    [Fact]
    public void ToString_FormatsCorrectly()
    {
        var column = new ColumnDefinition("id", DataType.Int, isPrimaryKey: true, isNullable: false);
        var str = column.ToString();

        Assert.Contains("id", str);
        Assert.Contains("Int", str);
        Assert.Contains("NOT NULL", str);
        Assert.Contains("PRIMARY KEY", str);
    }
}
