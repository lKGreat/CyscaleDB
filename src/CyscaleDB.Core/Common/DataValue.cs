using System.Text;

namespace CyscaleDB.Core.Common;

/// <summary>
/// Represents a typed data value that can hold any supported database type.
/// Provides serialization and deserialization capabilities.
/// </summary>
public readonly struct DataValue : IEquatable<DataValue>, IComparable<DataValue>
{
    private readonly object? _value;

    /// <summary>
    /// The data type of this value.
    /// </summary>
    public DataType Type { get; }

    /// <summary>
    /// Gets whether this value is NULL.
    /// </summary>
    public bool IsNull => _value is null || Type == DataType.Null;

    /// <summary>
    /// Creates a NULL value.
    /// </summary>
    public static DataValue Null => new(DataType.Null, null);

    private DataValue(DataType type, object? value)
    {
        Type = type;
        _value = value;
    }

    #region Factory Methods

    public static DataValue FromInt(int value) => new(DataType.Int, value);
    public static DataValue FromInt(int? value) => value.HasValue ? FromInt(value.Value) : Null;

    public static DataValue FromBigInt(long value) => new(DataType.BigInt, value);
    public static DataValue FromBigInt(long? value) => value.HasValue ? FromBigInt(value.Value) : Null;

    public static DataValue FromSmallInt(short value) => new(DataType.SmallInt, value);
    public static DataValue FromSmallInt(short? value) => value.HasValue ? FromSmallInt(value.Value) : Null;

    public static DataValue FromTinyInt(sbyte value) => new(DataType.TinyInt, value);
    public static DataValue FromTinyInt(sbyte? value) => value.HasValue ? FromTinyInt(value.Value) : Null;

    public static DataValue FromBoolean(bool value) => new(DataType.Boolean, value);
    public static DataValue FromBoolean(bool? value) => value.HasValue ? FromBoolean(value.Value) : Null;

    public static DataValue FromVarChar(string? value) => value is null ? Null : new(DataType.VarChar, value);
    public static DataValue FromChar(string? value) => value is null ? Null : new(DataType.Char, value);
    public static DataValue FromText(string? value) => value is null ? Null : new(DataType.Text, value);

    public static DataValue FromDateTime(DateTime value) => new(DataType.DateTime, value);
    public static DataValue FromDateTime(DateTime? value) => value.HasValue ? FromDateTime(value.Value) : Null;

    public static DataValue FromDate(DateOnly value) => new(DataType.Date, value);
    public static DataValue FromDate(DateOnly? value) => value.HasValue ? FromDate(value.Value) : Null;

    public static DataValue FromTime(TimeOnly value) => new(DataType.Time, value);
    public static DataValue FromTime(TimeOnly? value) => value.HasValue ? FromTime(value.Value) : Null;

    public static DataValue FromFloat(float value) => new(DataType.Float, value);
    public static DataValue FromFloat(float? value) => value.HasValue ? FromFloat(value.Value) : Null;

    public static DataValue FromDouble(double value) => new(DataType.Double, value);
    public static DataValue FromDouble(double? value) => value.HasValue ? FromDouble(value.Value) : Null;

    public static DataValue FromDecimal(decimal value) => new(DataType.Decimal, value);
    public static DataValue FromDecimal(decimal? value) => value.HasValue ? FromDecimal(value.Value) : Null;

    public static DataValue FromBlob(byte[]? value) => value is null ? Null : new(DataType.Blob, value);

    #endregion

    #region Value Accessors

    public int AsInt() => (int)(_value ?? throw new InvalidOperationException("Value is null"));
    public long AsBigInt() => (long)(_value ?? throw new InvalidOperationException("Value is null"));
    public short AsSmallInt() => (short)(_value ?? throw new InvalidOperationException("Value is null"));
    public sbyte AsTinyInt() => (sbyte)(_value ?? throw new InvalidOperationException("Value is null"));
    public bool AsBoolean() => (bool)(_value ?? throw new InvalidOperationException("Value is null"));
    public string AsString() => (string)(_value ?? throw new InvalidOperationException("Value is null"));
    public DateTime AsDateTime() => (DateTime)(_value ?? throw new InvalidOperationException("Value is null"));
    public DateOnly AsDate() => (DateOnly)(_value ?? throw new InvalidOperationException("Value is null"));
    public TimeOnly AsTime() => (TimeOnly)(_value ?? throw new InvalidOperationException("Value is null"));
    public float AsFloat() => (float)(_value ?? throw new InvalidOperationException("Value is null"));
    public double AsDouble() => (double)(_value ?? throw new InvalidOperationException("Value is null"));
    public decimal AsDecimal() => (decimal)(_value ?? throw new InvalidOperationException("Value is null"));
    public byte[] AsBlob() => (byte[])(_value ?? throw new InvalidOperationException("Value is null"));

    /// <summary>
    /// Gets the raw object value (can be null).
    /// </summary>
    public object? GetRawValue() => _value;

    #endregion

    #region Serialization

    /// <summary>
    /// Serializes this value to a byte array.
    /// Format: [1 byte type][1 byte null flag][data bytes]
    /// Variable-length types include a 4-byte length prefix before data.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // Write type
        writer.Write((byte)Type);

        // Write null flag
        writer.Write(IsNull);

        if (!IsNull)
        {
            WriteValueBytes(writer);
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Serializes only the value bytes (without type info) for storage in rows.
    /// Returns empty array for null values.
    /// </summary>
    public byte[] SerializeValue()
    {
        if (IsNull) return [];

        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        WriteValueBytes(writer);
        return stream.ToArray();
    }

    private void WriteValueBytes(BinaryWriter writer)
    {
        switch (Type)
        {
            case DataType.TinyInt:
                writer.Write(AsTinyInt());
                break;
            case DataType.SmallInt:
                writer.Write(AsSmallInt());
                break;
            case DataType.Int:
                writer.Write(AsInt());
                break;
            case DataType.BigInt:
                writer.Write(AsBigInt());
                break;
            case DataType.Boolean:
                writer.Write(AsBoolean());
                break;
            case DataType.Float:
                writer.Write(AsFloat());
                break;
            case DataType.Double:
                writer.Write(AsDouble());
                break;
            case DataType.Decimal:
                writer.Write(AsDecimal());
                break;
            case DataType.Date:
                writer.Write(AsDate().DayNumber);
                break;
            case DataType.Time:
                writer.Write(AsTime().Ticks);
                break;
            case DataType.DateTime:
            case DataType.Timestamp:
                writer.Write(AsDateTime().Ticks);
                break;
            case DataType.VarChar:
            case DataType.Char:
            case DataType.Text:
                var strBytes = Encoding.UTF8.GetBytes(AsString());
                writer.Write(strBytes.Length);
                writer.Write(strBytes);
                break;
            case DataType.Blob:
                var blobBytes = AsBlob();
                writer.Write(blobBytes.Length);
                writer.Write(blobBytes);
                break;
            case DataType.Null:
                // Nothing to write
                break;
            default:
                throw new NotSupportedException($"Serialization not supported for type {Type}");
        }
    }

    /// <summary>
    /// Deserializes a DataValue from a byte array (with type info).
    /// </summary>
    public static DataValue Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);
        return Deserialize(reader);
    }

    /// <summary>
    /// Deserializes a DataValue from a BinaryReader.
    /// </summary>
    public static DataValue Deserialize(BinaryReader reader)
    {
        var type = (DataType)reader.ReadByte();
        var isNull = reader.ReadBoolean();

        if (isNull)
        {
            return Null;
        }

        return DeserializeValue(reader, type);
    }

    /// <summary>
    /// Deserializes only the value bytes (without type info) given the expected type.
    /// </summary>
    public static DataValue DeserializeValue(byte[] data, DataType type)
    {
        if (data.Length == 0) return Null;

        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);
        return DeserializeValue(reader, type);
    }

    /// <summary>
    /// Deserializes a value from a BinaryReader given the expected type.
    /// </summary>
    public static DataValue DeserializeValue(BinaryReader reader, DataType type)
    {
        return type switch
        {
            DataType.TinyInt => FromTinyInt(reader.ReadSByte()),
            DataType.SmallInt => FromSmallInt(reader.ReadInt16()),
            DataType.Int => FromInt(reader.ReadInt32()),
            DataType.BigInt => FromBigInt(reader.ReadInt64()),
            DataType.Boolean => FromBoolean(reader.ReadBoolean()),
            DataType.Float => FromFloat(reader.ReadSingle()),
            DataType.Double => FromDouble(reader.ReadDouble()),
            DataType.Decimal => FromDecimal(reader.ReadDecimal()),
            DataType.Date => FromDate(DateOnly.FromDayNumber(reader.ReadInt32())),
            DataType.Time => FromTime(new TimeOnly(reader.ReadInt64())),
            DataType.DateTime => FromDateTime(new DateTime(reader.ReadInt64())),
            DataType.Timestamp => FromDateTime(new DateTime(reader.ReadInt64())),
            DataType.VarChar or DataType.Char or DataType.Text => DeserializeString(reader, type),
            DataType.Blob => DeserializeBlob(reader),
            DataType.Null => Null,
            _ => throw new NotSupportedException($"Deserialization not supported for type {type}")
        };
    }

    private static DataValue DeserializeString(BinaryReader reader, DataType type)
    {
        var length = reader.ReadInt32();
        var bytes = reader.ReadBytes(length);
        var str = Encoding.UTF8.GetString(bytes);
        return type switch
        {
            DataType.VarChar => FromVarChar(str),
            DataType.Char => FromChar(str),
            DataType.Text => FromText(str),
            _ => throw new InvalidOperationException()
        };
    }

    private static DataValue DeserializeBlob(BinaryReader reader)
    {
        var length = reader.ReadInt32();
        var bytes = reader.ReadBytes(length);
        return FromBlob(bytes);
    }

    #endregion

    #region Comparison and Equality

    public bool Equals(DataValue other)
    {
        if (IsNull && other.IsNull) return true;
        if (IsNull || other.IsNull) return false;
        if (Type != other.Type) return false;

        return Type switch
        {
            DataType.Blob => ((byte[])_value!).SequenceEqual((byte[])other._value!),
            _ => Equals(_value, other._value)
        };
    }

    public override bool Equals(object? obj) => obj is DataValue other && Equals(other);

    public override int GetHashCode()
    {
        if (IsNull) return 0;
        return HashCode.Combine(Type, _value);
    }

    public int CompareTo(DataValue other)
    {
        // Handle nulls
        if (IsNull && other.IsNull) return 0;
        if (IsNull) return -1;
        if (other.IsNull) return 1;

        // Type mismatch - compare type codes
        if (Type != other.Type)
        {
            // Try numeric comparison for compatible types
            if (Type.IsNumeric() && other.Type.IsNumeric())
            {
                return ToDouble().CompareTo(other.ToDouble());
            }
            return ((int)Type).CompareTo((int)other.Type);
        }

        return Type switch
        {
            DataType.TinyInt => AsTinyInt().CompareTo(other.AsTinyInt()),
            DataType.SmallInt => AsSmallInt().CompareTo(other.AsSmallInt()),
            DataType.Int => AsInt().CompareTo(other.AsInt()),
            DataType.BigInt => AsBigInt().CompareTo(other.AsBigInt()),
            DataType.Boolean => AsBoolean().CompareTo(other.AsBoolean()),
            DataType.Float => AsFloat().CompareTo(other.AsFloat()),
            DataType.Double => AsDouble().CompareTo(other.AsDouble()),
            DataType.Decimal => AsDecimal().CompareTo(other.AsDecimal()),
            DataType.Date => AsDate().CompareTo(other.AsDate()),
            DataType.Time => AsTime().CompareTo(other.AsTime()),
            DataType.DateTime or DataType.Timestamp => AsDateTime().CompareTo(other.AsDateTime()),
            DataType.VarChar or DataType.Char or DataType.Text => 
                string.Compare(AsString(), other.AsString(), StringComparison.Ordinal),
            DataType.Blob => CompareBlobs(AsBlob(), other.AsBlob()),
            _ => 0
        };
    }

    private static int CompareBlobs(byte[] a, byte[] b)
    {
        var minLen = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLen; i++)
        {
            var cmp = a[i].CompareTo(b[i]);
            if (cmp != 0) return cmp;
        }
        return a.Length.CompareTo(b.Length);
    }

    private double ToDouble()
    {
        return Type switch
        {
            DataType.TinyInt => AsTinyInt(),
            DataType.SmallInt => AsSmallInt(),
            DataType.Int => AsInt(),
            DataType.BigInt => AsBigInt(),
            DataType.Float => AsFloat(),
            DataType.Double => AsDouble(),
            DataType.Decimal => (double)AsDecimal(),
            _ => throw new InvalidOperationException($"Cannot convert {Type} to double")
        };
    }

    public static bool operator ==(DataValue left, DataValue right) => left.Equals(right);
    public static bool operator !=(DataValue left, DataValue right) => !left.Equals(right);
    public static bool operator <(DataValue left, DataValue right) => left.CompareTo(right) < 0;
    public static bool operator <=(DataValue left, DataValue right) => left.CompareTo(right) <= 0;
    public static bool operator >(DataValue left, DataValue right) => left.CompareTo(right) > 0;
    public static bool operator >=(DataValue left, DataValue right) => left.CompareTo(right) >= 0;

    #endregion

    public override string ToString()
    {
        if (IsNull) return "NULL";
        return Type switch
        {
            DataType.VarChar or DataType.Char or DataType.Text => $"'{AsString()}'",
            DataType.DateTime or DataType.Timestamp => AsDateTime().ToString("yyyy-MM-dd HH:mm:ss"),
            DataType.Date => AsDate().ToString("yyyy-MM-dd"),
            DataType.Time => AsTime().ToString("HH:mm:ss"),
            DataType.Boolean => AsBoolean() ? "TRUE" : "FALSE",
            DataType.Blob => $"BLOB({AsBlob().Length} bytes)",
            _ => _value?.ToString() ?? "NULL"
        };
    }
}
