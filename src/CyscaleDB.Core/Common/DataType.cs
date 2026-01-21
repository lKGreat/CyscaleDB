namespace CyscaleDB.Core.Common;

/// <summary>
/// Supported data types in CyscaleDB.
/// </summary>
public enum DataType
{
    /// <summary>
    /// 32-bit signed integer (-2,147,483,648 to 2,147,483,647)
    /// </summary>
    Int = 1,

    /// <summary>
    /// 64-bit signed integer (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
    /// </summary>
    BigInt = 2,

    /// <summary>
    /// 16-bit signed integer (-32,768 to 32,767)
    /// </summary>
    SmallInt = 3,

    /// <summary>
    /// 8-bit signed integer (-128 to 127)
    /// </summary>
    TinyInt = 4,

    /// <summary>
    /// Variable-length character string with maximum length
    /// </summary>
    VarChar = 10,

    /// <summary>
    /// Fixed-length character string
    /// </summary>
    Char = 11,

    /// <summary>
    /// Large text data
    /// </summary>
    Text = 12,

    /// <summary>
    /// Boolean value (true/false)
    /// </summary>
    Boolean = 20,

    /// <summary>
    /// Date and time (year, month, day, hour, minute, second)
    /// </summary>
    DateTime = 30,

    /// <summary>
    /// Date only (year, month, day)
    /// </summary>
    Date = 31,

    /// <summary>
    /// Time only (hour, minute, second)
    /// </summary>
    Time = 32,

    /// <summary>
    /// Timestamp with timezone information
    /// </summary>
    Timestamp = 33,

    /// <summary>
    /// Single-precision floating point (32-bit)
    /// </summary>
    Float = 40,

    /// <summary>
    /// Double-precision floating point (64-bit)
    /// </summary>
    Double = 41,

    /// <summary>
    /// Fixed-point decimal with precision and scale
    /// </summary>
    Decimal = 42,

    /// <summary>
    /// Binary large object
    /// </summary>
    Blob = 50,

    /// <summary>
    /// JSON data (stored as text, validated and parsed)
    /// </summary>
    Json = 51,

    /// <summary>
    /// Geometry/spatial data
    /// </summary>
    Geometry = 60,

    /// <summary>
    /// NULL type (internal use)
    /// </summary>
    Null = 0
}

/// <summary>
/// Extension methods for DataType enum.
/// </summary>
public static class DataTypeExtensions
{
    /// <summary>
    /// Determines if the data type is a fixed-length type.
    /// </summary>
    public static bool IsFixedLength(this DataType dataType)
    {
        return dataType switch
        {
            DataType.Int => true,
            DataType.BigInt => true,
            DataType.SmallInt => true,
            DataType.TinyInt => true,
            DataType.Boolean => true,
            DataType.DateTime => true,
            DataType.Date => true,
            DataType.Time => true,
            DataType.Timestamp => true,
            DataType.Float => true,
            DataType.Double => true,
            DataType.Char => true,
            _ => false
        };
    }

    /// <summary>
    /// Gets the fixed byte size for fixed-length types.
    /// Returns -1 for variable-length types.
    /// </summary>
    public static int GetFixedByteSize(this DataType dataType)
    {
        return dataType switch
        {
            DataType.TinyInt => 1,
            DataType.SmallInt => 2,
            DataType.Int => 4,
            DataType.BigInt => 8,
            DataType.Boolean => 1,
            DataType.Float => 4,
            DataType.Double => 8,
            DataType.Date => 4,      // Days since epoch
            DataType.Time => 8,      // Ticks
            DataType.DateTime => 8,  // Ticks
            DataType.Timestamp => 8, // Ticks
            _ => -1 // Variable length
        };
    }

    /// <summary>
    /// Determines if the data type is numeric.
    /// </summary>
    public static bool IsNumeric(this DataType dataType)
    {
        return dataType switch
        {
            DataType.Int => true,
            DataType.BigInt => true,
            DataType.SmallInt => true,
            DataType.TinyInt => true,
            DataType.Float => true,
            DataType.Double => true,
            DataType.Decimal => true,
            _ => false
        };
    }

    /// <summary>
    /// Determines if the data type is a string type.
    /// </summary>
    public static bool IsString(this DataType dataType)
    {
        return dataType switch
        {
            DataType.VarChar => true,
            DataType.Char => true,
            DataType.Text => true,
            _ => false
        };
    }

    /// <summary>
    /// Determines if the data type is a temporal type.
    /// </summary>
    public static bool IsTemporal(this DataType dataType)
    {
        return dataType switch
        {
            DataType.DateTime => true,
            DataType.Date => true,
            DataType.Time => true,
            DataType.Timestamp => true,
            _ => false
        };
    }
}
