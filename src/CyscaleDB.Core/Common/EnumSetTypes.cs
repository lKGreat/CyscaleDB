namespace CyscaleDB.Core.Common;

/// <summary>
/// Defines an ENUM type with a fixed set of allowed string values.
/// Values are stored as integer indices for efficiency.
/// </summary>
public sealed class EnumTypeDefinition
{
    /// <summary>
    /// Gets the name of this ENUM type.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the allowed values for this ENUM type.
    /// Values are stored in order, and indices are 1-based (like MySQL).
    /// </summary>
    public IReadOnlyList<string> Values { get; }

    /// <summary>
    /// Gets the maximum valid index (1-based).
    /// </summary>
    public int MaxIndex => Values.Count;

    /// <summary>
    /// Creates a new ENUM type definition.
    /// </summary>
    /// <param name="name">The name of the ENUM type</param>
    /// <param name="values">The allowed values</param>
    public EnumTypeDefinition(string name, IEnumerable<string> values)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("ENUM type name cannot be empty", nameof(name));

        var valueList = values.ToList();
        if (valueList.Count == 0)
            throw new ArgumentException("ENUM type must have at least one value", nameof(values));

        if (valueList.Count > 65535)
            throw new ArgumentException("ENUM type cannot have more than 65535 values", nameof(values));

        Name = name;
        Values = valueList.AsReadOnly();
    }

    /// <summary>
    /// Parses a string value to its ENUM index (1-based).
    /// </summary>
    /// <param name="value">The string value to parse</param>
    /// <returns>The 1-based index of the value</returns>
    /// <exception cref="ArgumentException">If the value is not in the ENUM</exception>
    public int Parse(string value)
    {
        var index = Values.ToList().FindIndex(v => 
            v.Equals(value, StringComparison.OrdinalIgnoreCase));
        
        if (index < 0)
            throw new ArgumentException($"Invalid ENUM value: '{value}'. Allowed values: {string.Join(", ", Values)}");

        return index + 1; // 1-based index like MySQL
    }

    /// <summary>
    /// Tries to parse a string value to its ENUM index.
    /// </summary>
    /// <param name="value">The string value to parse</param>
    /// <param name="index">The 1-based index if successful</param>
    /// <returns>True if the value was found</returns>
    public bool TryParse(string value, out int index)
    {
        var idx = Values.ToList().FindIndex(v => 
            v.Equals(value, StringComparison.OrdinalIgnoreCase));
        
        if (idx < 0)
        {
            index = 0;
            return false;
        }

        index = idx + 1;
        return true;
    }

    /// <summary>
    /// Gets the string value for an ENUM index.
    /// </summary>
    /// <param name="index">The 1-based index</param>
    /// <returns>The string value</returns>
    /// <exception cref="ArgumentOutOfRangeException">If the index is invalid</exception>
    public string GetValue(int index)
    {
        if (index < 1 || index > Values.Count)
            throw new ArgumentOutOfRangeException(nameof(index), 
                $"ENUM index must be between 1 and {Values.Count}");

        return Values[index - 1];
    }

    /// <summary>
    /// Creates a DataValue from a string ENUM value.
    /// Stores the index (1-based) as an integer.
    /// </summary>
    public DataValue CreateDataValue(string value)
    {
        var index = Parse(value);
        return DataValue.FromInt(index);
    }

    /// <summary>
    /// Creates a DataValue from an ENUM index.
    /// Stores the index (1-based) as an integer.
    /// </summary>
    public DataValue CreateDataValue(int index)
    {
        // Validate index
        _ = GetValue(index);
        return DataValue.FromInt(index);
    }

    /// <summary>
    /// Serializes this ENUM type definition.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(Name);
        writer.Write(Values.Count);
        foreach (var value in Values)
        {
            writer.Write(value);
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes an ENUM type definition.
    /// </summary>
    public static EnumTypeDefinition Deserialize(BinaryReader reader)
    {
        var name = reader.ReadString();
        var count = reader.ReadInt32();
        var values = new List<string>(count);
        
        for (int i = 0; i < count; i++)
        {
            values.Add(reader.ReadString());
        }

        return new EnumTypeDefinition(name, values);
    }

    public override string ToString()
    {
        return $"ENUM('{string.Join("', '", Values)}')";
    }
}

/// <summary>
/// Defines a SET type that can hold zero or more values from a fixed set.
/// Values are stored as a bitmask for efficiency.
/// </summary>
public sealed class SetTypeDefinition
{
    /// <summary>
    /// Gets the name of this SET type.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the allowed values for this SET type.
    /// Each value corresponds to a bit position.
    /// </summary>
    public IReadOnlyList<string> Values { get; }

    /// <summary>
    /// Gets the maximum number of values (limited to 64).
    /// </summary>
    public int MaxValues => Values.Count;

    /// <summary>
    /// Creates a new SET type definition.
    /// </summary>
    /// <param name="name">The name of the SET type</param>
    /// <param name="values">The allowed values</param>
    public SetTypeDefinition(string name, IEnumerable<string> values)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("SET type name cannot be empty", nameof(name));

        var valueList = values.ToList();
        if (valueList.Count == 0)
            throw new ArgumentException("SET type must have at least one value", nameof(values));

        if (valueList.Count > 64)
            throw new ArgumentException("SET type cannot have more than 64 values", nameof(values));

        Name = name;
        Values = valueList.AsReadOnly();
    }

    /// <summary>
    /// Parses a comma-separated string of values to a bitmask.
    /// </summary>
    /// <param name="commaSeparatedValues">The comma-separated values</param>
    /// <returns>The bitmask representing the set</returns>
    public long Parse(string commaSeparatedValues)
    {
        if (string.IsNullOrWhiteSpace(commaSeparatedValues))
            return 0;

        var parts = commaSeparatedValues.Split(',', StringSplitOptions.RemoveEmptyEntries);
        long bitmap = 0;

        foreach (var part in parts)
        {
            var value = part.Trim();
            var index = Values.ToList().FindIndex(v => 
                v.Equals(value, StringComparison.OrdinalIgnoreCase));

            if (index < 0)
                throw new ArgumentException($"Invalid SET value: '{value}'. Allowed values: {string.Join(", ", Values)}");

            bitmap |= (1L << index);
        }

        return bitmap;
    }

    /// <summary>
    /// Tries to parse a comma-separated string of values to a bitmask.
    /// </summary>
    public bool TryParse(string commaSeparatedValues, out long bitmap)
    {
        bitmap = 0;
        
        if (string.IsNullOrWhiteSpace(commaSeparatedValues))
            return true;

        var parts = commaSeparatedValues.Split(',', StringSplitOptions.RemoveEmptyEntries);

        foreach (var part in parts)
        {
            var value = part.Trim();
            var index = Values.ToList().FindIndex(v => 
                v.Equals(value, StringComparison.OrdinalIgnoreCase));

            if (index < 0)
                return false;

            bitmap |= (1L << index);
        }

        return true;
    }

    /// <summary>
    /// Converts a bitmask to its comma-separated string representation.
    /// </summary>
    /// <param name="bitmap">The bitmask</param>
    /// <returns>Comma-separated string of set values</returns>
    public string GetValues(long bitmap)
    {
        var result = new List<string>();

        for (int i = 0; i < Values.Count; i++)
        {
            if ((bitmap & (1L << i)) != 0)
            {
                result.Add(Values[i]);
            }
        }

        return string.Join(",", result);
    }

    /// <summary>
    /// Checks if a bitmask contains a specific value.
    /// </summary>
    public bool Contains(long bitmap, string value)
    {
        var index = Values.ToList().FindIndex(v => 
            v.Equals(value, StringComparison.OrdinalIgnoreCase));

        if (index < 0)
            return false;

        return (bitmap & (1L << index)) != 0;
    }

    /// <summary>
    /// Creates a DataValue from a comma-separated string of SET values.
    /// Stores the bitmask as a bigint.
    /// </summary>
    public DataValue CreateDataValue(string commaSeparatedValues)
    {
        var bitmap = Parse(commaSeparatedValues);
        return DataValue.FromBigInt(bitmap);
    }

    /// <summary>
    /// Creates a DataValue from a bitmask.
    /// Stores the bitmask as a bigint.
    /// </summary>
    public DataValue CreateDataValue(long bitmap)
    {
        return DataValue.FromBigInt(bitmap);
    }

    /// <summary>
    /// Serializes this SET type definition.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(Name);
        writer.Write(Values.Count);
        foreach (var value in Values)
        {
            writer.Write(value);
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a SET type definition.
    /// </summary>
    public static SetTypeDefinition Deserialize(BinaryReader reader)
    {
        var name = reader.ReadString();
        var count = reader.ReadInt32();
        var values = new List<string>(count);
        
        for (int i = 0; i < count; i++)
        {
            values.Add(reader.ReadString());
        }

        return new SetTypeDefinition(name, values);
    }

    public override string ToString()
    {
        return $"SET('{string.Join("', '", Values)}')";
    }
}
