using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Defines the metadata for a column in a table.
/// </summary>
public sealed class ColumnDefinition
{
    /// <summary>
    /// The name of the column.
    /// </summary>
    public string Name { get; internal set; }

    /// <summary>
    /// The data type of the column.
    /// </summary>
    public DataType DataType { get; }

    /// <summary>
    /// The maximum length for variable-length types (VARCHAR, CHAR).
    /// For fixed-length types, this is 0.
    /// </summary>
    public int MaxLength { get; }

    /// <summary>
    /// The precision for DECIMAL type (total number of digits).
    /// </summary>
    public int Precision { get; }

    /// <summary>
    /// The scale for DECIMAL type (number of digits after decimal point).
    /// </summary>
    public int Scale { get; }

    /// <summary>
    /// Whether the column allows NULL values.
    /// </summary>
    public bool IsNullable { get; }

    /// <summary>
    /// Whether this column is a primary key.
    /// </summary>
    public bool IsPrimaryKey { get; }

    /// <summary>
    /// Whether this column has auto-increment enabled.
    /// </summary>
    public bool IsAutoIncrement { get; }

    /// <summary>
    /// The default value for this column, or null if none.
    /// </summary>
    public DataValue? DefaultValue { get; }

    /// <summary>
    /// The ordinal position of this column in the table (0-based).
    /// </summary>
    public int OrdinalPosition { get; internal set; }

    /// <summary>
    /// The ENUM type definition if this column is of type ENUM.
    /// </summary>
    public EnumTypeDefinition? EnumType { get; }

    /// <summary>
    /// The SET type definition if this column is of type SET.
    /// </summary>
    public SetTypeDefinition? SetType { get; }

    /// <summary>
    /// Creates a new column definition.
    /// </summary>
    public ColumnDefinition(
        string name,
        DataType dataType,
        int maxLength = 0,
        int precision = 0,
        int scale = 0,
        bool isNullable = true,
        bool isPrimaryKey = false,
        bool isAutoIncrement = false,
        DataValue? defaultValue = null,
        EnumTypeDefinition? enumType = null,
        SetTypeDefinition? setType = null)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be empty", nameof(name));

        if (name.Length > Constants.MaxColumnNameLength)
            throw new ArgumentException($"Column name exceeds maximum length of {Constants.MaxColumnNameLength}", nameof(name));

        // Validate ENUM/SET type constraints
        if (dataType == DataType.Enum && enumType == null)
            throw new ArgumentException("ENUM column requires an EnumTypeDefinition", nameof(enumType));
        if (dataType == DataType.Set && setType == null)
            throw new ArgumentException("SET column requires a SetTypeDefinition", nameof(setType));

        Name = name;
        DataType = dataType;
        MaxLength = maxLength > 0 ? maxLength : GetDefaultMaxLength(dataType);
        Precision = precision;
        Scale = scale;
        IsNullable = isNullable && !isPrimaryKey; // Primary keys cannot be nullable
        IsPrimaryKey = isPrimaryKey;
        IsAutoIncrement = isAutoIncrement;
        DefaultValue = defaultValue;
        EnumType = enumType;
        SetType = setType;
    }

    private static int GetDefaultMaxLength(DataType dataType)
    {
        return dataType switch
        {
            DataType.VarChar => Constants.DefaultVarCharLength,
            DataType.Char => 1,
            DataType.Text => Constants.MaxVarCharLength,
            _ => 0
        };
    }

    /// <summary>
    /// Gets the byte size of this column for fixed-length types,
    /// or the maximum possible size for variable-length types.
    /// </summary>
    public int GetByteSize()
    {
        var fixedSize = DataType.GetFixedByteSize();
        if (fixedSize > 0) return fixedSize;

        return DataType switch
        {
            DataType.VarChar or DataType.Char => MaxLength * 4 + 4, // UTF-8 worst case + length prefix
            DataType.Text => Constants.MaxVarCharLength + 4,
            DataType.Blob => int.MaxValue, // Variable
            DataType.Decimal => 16, // decimal is 16 bytes
            _ => 0
        };
    }

    /// <summary>
    /// Validates that a value is compatible with this column definition.
    /// </summary>
    public bool ValidateValue(DataValue value)
    {
        if (value.IsNull)
        {
            return IsNullable;
        }

        // Check type compatibility
        if (value.Type != DataType)
        {
            // Allow compatible numeric types
            if (DataType.IsNumeric() && value.Type.IsNumeric())
            {
                return true;
            }
            // Allow compatible string types
            if (DataType.IsString() && value.Type.IsString())
            {
                return true;
            }
            return false;
        }

        // Check length constraints for strings
        if (DataType.IsString() && MaxLength > 0)
        {
            var str = value.AsString();
            if (str.Length > MaxLength)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Serializes this column definition to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(Name);
        writer.Write((byte)DataType);
        writer.Write(MaxLength);
        writer.Write(Precision);
        writer.Write(Scale);
        writer.Write(IsNullable);
        writer.Write(IsPrimaryKey);
        writer.Write(IsAutoIncrement);
        writer.Write(OrdinalPosition);

        // Write default value
        var hasDefault = DefaultValue.HasValue;
        writer.Write(hasDefault);
        if (hasDefault)
        {
            var defaultBytes = DefaultValue!.Value.Serialize();
            writer.Write(defaultBytes.Length);
            writer.Write(defaultBytes);
        }

        // Write ENUM type if present
        writer.Write(EnumType != null);
        if (EnumType != null)
        {
            var enumBytes = EnumType.Serialize();
            writer.Write(enumBytes.Length);
            writer.Write(enumBytes);
        }

        // Write SET type if present
        writer.Write(SetType != null);
        if (SetType != null)
        {
            var setBytes = SetType.Serialize();
            writer.Write(setBytes.Length);
            writer.Write(setBytes);
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a column definition from bytes.
    /// </summary>
    public static ColumnDefinition Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);
        return Deserialize(reader);
    }

    /// <summary>
    /// Deserializes a column definition from a BinaryReader.
    /// </summary>
    public static ColumnDefinition Deserialize(BinaryReader reader)
    {
        var name = reader.ReadString();
        var dataType = (DataType)reader.ReadByte();
        var maxLength = reader.ReadInt32();
        var precision = reader.ReadInt32();
        var scale = reader.ReadInt32();
        var isNullable = reader.ReadBoolean();
        var isPrimaryKey = reader.ReadBoolean();
        var isAutoIncrement = reader.ReadBoolean();
        var ordinalPosition = reader.ReadInt32();

        DataValue? defaultValue = null;
        var hasDefault = reader.ReadBoolean();
        if (hasDefault)
        {
            var defaultLength = reader.ReadInt32();
            var defaultBytes = reader.ReadBytes(defaultLength);
            defaultValue = DataValue.Deserialize(defaultBytes);
        }

        // Read ENUM type if present
        EnumTypeDefinition? enumType = null;
        if (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            var hasEnum = reader.ReadBoolean();
            if (hasEnum)
            {
                var enumLength = reader.ReadInt32();
                var enumBytes = reader.ReadBytes(enumLength);
                using var enumStream = new MemoryStream(enumBytes);
                using var enumReader = new BinaryReader(enumStream);
                enumType = EnumTypeDefinition.Deserialize(enumReader);
            }
        }

        // Read SET type if present
        SetTypeDefinition? setType = null;
        if (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            var hasSet = reader.ReadBoolean();
            if (hasSet)
            {
                var setLength = reader.ReadInt32();
                var setBytes = reader.ReadBytes(setLength);
                using var setStream = new MemoryStream(setBytes);
                using var setReader = new BinaryReader(setStream);
                setType = SetTypeDefinition.Deserialize(setReader);
            }
        }

        var column = new ColumnDefinition(
            name, dataType, maxLength, precision, scale,
            isNullable, isPrimaryKey, isAutoIncrement, defaultValue,
            enumType, setType)
        {
            OrdinalPosition = ordinalPosition
        };

        return column;
    }

    public override string ToString()
    {
        var parts = new List<string> { Name, DataType.ToString() };

        if (DataType.IsString() && MaxLength > 0)
        {
            parts[1] = $"{DataType}({MaxLength})";
        }
        else if (DataType == DataType.Decimal && (Precision > 0 || Scale > 0))
        {
            parts[1] = $"DECIMAL({Precision},{Scale})";
        }

        if (!IsNullable) parts.Add("NOT NULL");
        if (IsPrimaryKey) parts.Add("PRIMARY KEY");
        if (IsAutoIncrement) parts.Add("AUTO_INCREMENT");
        if (DefaultValue.HasValue) parts.Add($"DEFAULT {DefaultValue.Value}");

        return string.Join(" ", parts);
    }
}
