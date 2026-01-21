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
    /// 24-bit signed integer (-8,388,608 to 8,388,607)
    /// </summary>
    MediumInt = 5,

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
    /// Tiny text data (max 255 bytes)
    /// </summary>
    TinyText = 13,

    /// <summary>
    /// Medium text data (max 16MB)
    /// </summary>
    MediumText = 14,

    /// <summary>
    /// Long text data (max 4GB)
    /// </summary>
    LongText = 15,

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
    /// Year (1901 to 2155, or 0000)
    /// </summary>
    Year = 34,

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
    /// Tiny binary large object (max 255 bytes)
    /// </summary>
    TinyBlob = 52,

    /// <summary>
    /// Medium binary large object (max 16MB)
    /// </summary>
    MediumBlob = 53,

    /// <summary>
    /// Long binary large object (max 4GB)
    /// </summary>
    LongBlob = 54,

    /// <summary>
    /// Variable-length binary data
    /// </summary>
    VarBinary = 55,

    /// <summary>
    /// Fixed-length binary data
    /// </summary>
    Binary = 56,

    /// <summary>
    /// Geometry/spatial data
    /// </summary>
    Geometry = 60,

    /// <summary>
    /// ENUM type - fixed set of string values stored as integer index
    /// </summary>
    Enum = 70,

    /// <summary>
    /// SET type - zero or more values from a fixed set stored as bitmask
    /// </summary>
    Set = 71,

    /// <summary>
    /// BIT type - bit-field type (1 to 64 bits)
    /// </summary>
    Bit = 80,

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
            DataType.MediumInt => true,
            DataType.Boolean => true,
            DataType.DateTime => true,
            DataType.Date => true,
            DataType.Time => true,
            DataType.Timestamp => true,
            DataType.Year => true,
            DataType.Float => true,
            DataType.Double => true,
            DataType.Char => true,
            DataType.Binary => true,
            DataType.Bit => true,
            DataType.Enum => true,  // Stored as 4-byte int index
            DataType.Set => true,   // Stored as 8-byte long bitmap
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
            DataType.MediumInt => 3,
            DataType.Int => 4,
            DataType.BigInt => 8,
            DataType.Boolean => 1,
            DataType.Float => 4,
            DataType.Double => 8,
            DataType.Date => 4,      // Days since epoch
            DataType.Time => 8,      // Ticks
            DataType.DateTime => 8,  // Ticks
            DataType.Timestamp => 8, // Ticks
            DataType.Year => 2,      // 2-byte year value
            DataType.Bit => 8,       // Up to 64 bits stored as 8 bytes
            DataType.Enum => 4,      // Stored as int index
            DataType.Set => 8,       // Stored as long bitmap
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
            DataType.MediumInt => true,
            DataType.Float => true,
            DataType.Double => true,
            DataType.Decimal => true,
            DataType.Bit => true,
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
            DataType.TinyText => true,
            DataType.MediumText => true,
            DataType.LongText => true,
            _ => false
        };
    }

    /// <summary>
    /// Determines if the data type is a binary type.
    /// </summary>
    public static bool IsBinary(this DataType dataType)
    {
        return dataType switch
        {
            DataType.Blob => true,
            DataType.TinyBlob => true,
            DataType.MediumBlob => true,
            DataType.LongBlob => true,
            DataType.VarBinary => true,
            DataType.Binary => true,
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
            DataType.Year => true,
            _ => false
        };
    }
}
