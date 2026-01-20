using CyscaleDB.Core.Common;

namespace CyscaleDB.Tests;

public class DataValueTests
{
    #region Factory Methods and Value Access

    [Fact]
    public void Null_CreatesNullValue()
    {
        var value = DataValue.Null;
        Assert.True(value.IsNull);
        Assert.Equal(DataType.Null, value.Type);
    }

    [Fact]
    public void FromInt_CreatesIntValue()
    {
        var value = DataValue.FromInt(42);
        Assert.False(value.IsNull);
        Assert.Equal(DataType.Int, value.Type);
        Assert.Equal(42, value.AsInt());
    }

    [Fact]
    public void FromBigInt_CreatesBigIntValue()
    {
        var value = DataValue.FromBigInt(9223372036854775807L);
        Assert.Equal(DataType.BigInt, value.Type);
        Assert.Equal(9223372036854775807L, value.AsBigInt());
    }

    [Fact]
    public void FromBoolean_CreatesBooleanValue()
    {
        var trueValue = DataValue.FromBoolean(true);
        var falseValue = DataValue.FromBoolean(false);

        Assert.True(trueValue.AsBoolean());
        Assert.False(falseValue.AsBoolean());
    }

    [Fact]
    public void FromVarChar_CreatesStringValue()
    {
        var value = DataValue.FromVarChar("Hello, World!");
        Assert.Equal(DataType.VarChar, value.Type);
        Assert.Equal("Hello, World!", value.AsString());
    }

    [Fact]
    public void FromVarChar_WithNull_ReturnsNullValue()
    {
        var value = DataValue.FromVarChar(null);
        Assert.True(value.IsNull);
    }

    [Fact]
    public void FromDateTime_CreatesDateTimeValue()
    {
        var dt = new DateTime(2024, 6, 15, 10, 30, 45);
        var value = DataValue.FromDateTime(dt);
        Assert.Equal(DataType.DateTime, value.Type);
        Assert.Equal(dt, value.AsDateTime());
    }

    [Fact]
    public void FromFloat_CreatesFloatValue()
    {
        var value = DataValue.FromFloat(3.14f);
        Assert.Equal(DataType.Float, value.Type);
        Assert.Equal(3.14f, value.AsFloat());
    }

    [Fact]
    public void FromDouble_CreatesDoubleValue()
    {
        var value = DataValue.FromDouble(3.14159265359);
        Assert.Equal(DataType.Double, value.Type);
        Assert.Equal(3.14159265359, value.AsDouble());
    }

    [Fact]
    public void FromDecimal_CreatesDecimalValue()
    {
        var value = DataValue.FromDecimal(123.456m);
        Assert.Equal(DataType.Decimal, value.Type);
        Assert.Equal(123.456m, value.AsDecimal());
    }

    [Fact]
    public void FromBlob_CreatesBlobValue()
    {
        var bytes = new byte[] { 1, 2, 3, 4, 5 };
        var value = DataValue.FromBlob(bytes);
        Assert.Equal(DataType.Blob, value.Type);
        Assert.Equal(bytes, value.AsBlob());
    }

    #endregion

    #region Serialization

    [Theory]
    [InlineData(42)]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void Serialize_Int_RoundTrips(int input)
    {
        var original = DataValue.FromInt(input);
        var bytes = original.Serialize();
        var restored = DataValue.Deserialize(bytes);

        Assert.Equal(original.Type, restored.Type);
        Assert.Equal(original.AsInt(), restored.AsInt());
    }

    [Theory]
    [InlineData("")]
    [InlineData("Hello")]
    [InlineData("Hello, 世界!")]
    [InlineData("Special chars: \t\n\r")]
    public void Serialize_VarChar_RoundTrips(string input)
    {
        var original = DataValue.FromVarChar(input);
        var bytes = original.Serialize();
        var restored = DataValue.Deserialize(bytes);

        Assert.Equal(original.Type, restored.Type);
        Assert.Equal(original.AsString(), restored.AsString());
    }

    [Fact]
    public void Serialize_Null_RoundTrips()
    {
        var original = DataValue.Null;
        var bytes = original.Serialize();
        var restored = DataValue.Deserialize(bytes);

        Assert.True(restored.IsNull);
    }

    [Fact]
    public void Serialize_DateTime_RoundTrips()
    {
        var dt = new DateTime(2024, 6, 15, 10, 30, 45, DateTimeKind.Utc);
        var original = DataValue.FromDateTime(dt);
        var bytes = original.Serialize();
        var restored = DataValue.Deserialize(bytes);

        Assert.Equal(dt, restored.AsDateTime());
    }

    [Fact]
    public void Serialize_Decimal_RoundTrips()
    {
        var original = DataValue.FromDecimal(12345.6789m);
        var bytes = original.Serialize();
        var restored = DataValue.Deserialize(bytes);

        Assert.Equal(12345.6789m, restored.AsDecimal());
    }

    [Fact]
    public void SerializeValue_WithoutTypeInfo_Works()
    {
        var original = DataValue.FromInt(42);
        var bytes = original.SerializeValue();
        var restored = DataValue.DeserializeValue(bytes, DataType.Int);

        Assert.Equal(42, restored.AsInt());
    }

    #endregion

    #region Comparison

    [Fact]
    public void Equals_SameValues_ReturnsTrue()
    {
        var a = DataValue.FromInt(42);
        var b = DataValue.FromInt(42);
        Assert.True(a.Equals(b));
        Assert.True(a == b);
    }

    [Fact]
    public void Equals_DifferentValues_ReturnsFalse()
    {
        var a = DataValue.FromInt(42);
        var b = DataValue.FromInt(43);
        Assert.False(a.Equals(b));
        Assert.True(a != b);
    }

    [Fact]
    public void Equals_BothNull_ReturnsTrue()
    {
        var a = DataValue.Null;
        var b = DataValue.Null;
        Assert.True(a.Equals(b));
    }

    [Fact]
    public void CompareTo_Numbers_OrdersCorrectly()
    {
        var a = DataValue.FromInt(10);
        var b = DataValue.FromInt(20);
        var c = DataValue.FromInt(10);

        Assert.True(a < b);
        Assert.True(b > a);
        Assert.True(a <= c);
        Assert.True(a >= c);
    }

    [Fact]
    public void CompareTo_Strings_OrdersCorrectly()
    {
        var a = DataValue.FromVarChar("apple");
        var b = DataValue.FromVarChar("banana");

        Assert.True(a < b);
        Assert.True(b > a);
    }

    [Fact]
    public void CompareTo_NullLessThanNonNull()
    {
        var nullValue = DataValue.Null;
        var intValue = DataValue.FromInt(0);

        Assert.True(nullValue < intValue);
    }

    #endregion

    #region ToString

    [Fact]
    public void ToString_Null_ReturnsNULL()
    {
        Assert.Equal("NULL", DataValue.Null.ToString());
    }

    [Fact]
    public void ToString_Int_ReturnsNumber()
    {
        Assert.Equal("42", DataValue.FromInt(42).ToString());
    }

    [Fact]
    public void ToString_String_ReturnsQuoted()
    {
        Assert.Equal("'Hello'", DataValue.FromVarChar("Hello").ToString());
    }

    [Fact]
    public void ToString_Boolean_ReturnsTrueOrFalse()
    {
        Assert.Equal("TRUE", DataValue.FromBoolean(true).ToString());
        Assert.Equal("FALSE", DataValue.FromBoolean(false).ToString());
    }

    #endregion
}
