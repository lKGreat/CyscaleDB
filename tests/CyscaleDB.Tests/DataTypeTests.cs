using CyscaleDB.Core.Common;

namespace CyscaleDB.Tests;

public class DataTypeTests
{
    [Theory]
    [InlineData(DataType.Int, true)]
    [InlineData(DataType.BigInt, true)]
    [InlineData(DataType.SmallInt, true)]
    [InlineData(DataType.TinyInt, true)]
    [InlineData(DataType.Boolean, true)]
    [InlineData(DataType.Float, true)]
    [InlineData(DataType.Double, true)]
    [InlineData(DataType.DateTime, true)]
    [InlineData(DataType.VarChar, false)]
    [InlineData(DataType.Text, false)]
    [InlineData(DataType.Blob, false)]
    public void IsFixedLength_ReturnsCorrectValue(DataType type, bool expected)
    {
        Assert.Equal(expected, type.IsFixedLength());
    }

    [Theory]
    [InlineData(DataType.Int, 4)]
    [InlineData(DataType.BigInt, 8)]
    [InlineData(DataType.SmallInt, 2)]
    [InlineData(DataType.TinyInt, 1)]
    [InlineData(DataType.Boolean, 1)]
    [InlineData(DataType.Float, 4)]
    [InlineData(DataType.Double, 8)]
    [InlineData(DataType.VarChar, -1)]
    public void GetFixedByteSize_ReturnsCorrectSize(DataType type, int expected)
    {
        Assert.Equal(expected, type.GetFixedByteSize());
    }

    [Theory]
    [InlineData(DataType.Int, true)]
    [InlineData(DataType.BigInt, true)]
    [InlineData(DataType.Float, true)]
    [InlineData(DataType.Double, true)]
    [InlineData(DataType.Decimal, true)]
    [InlineData(DataType.VarChar, false)]
    [InlineData(DataType.Boolean, false)]
    public void IsNumeric_ReturnsCorrectValue(DataType type, bool expected)
    {
        Assert.Equal(expected, type.IsNumeric());
    }

    [Theory]
    [InlineData(DataType.VarChar, true)]
    [InlineData(DataType.Char, true)]
    [InlineData(DataType.Text, true)]
    [InlineData(DataType.Int, false)]
    [InlineData(DataType.Blob, false)]
    public void IsString_ReturnsCorrectValue(DataType type, bool expected)
    {
        Assert.Equal(expected, type.IsString());
    }

    [Theory]
    [InlineData(DataType.DateTime, true)]
    [InlineData(DataType.Date, true)]
    [InlineData(DataType.Time, true)]
    [InlineData(DataType.Timestamp, true)]
    [InlineData(DataType.Int, false)]
    [InlineData(DataType.VarChar, false)]
    public void IsTemporal_ReturnsCorrectValue(DataType type, bool expected)
    {
        Assert.Equal(expected, type.IsTemporal());
    }
}
