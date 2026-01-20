using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents a row of data in a table.
/// </summary>
public sealed class Row
{
    /// <summary>
    /// The schema of the table this row belongs to.
    /// </summary>
    public TableSchema Schema { get; }

    /// <summary>
    /// The values in this row, indexed by column ordinal.
    /// </summary>
    public DataValue[] Values { get; }

    /// <summary>
    /// The row identifier (page ID and slot number).
    /// </summary>
    public RowId RowId { get; internal set; }

    /// <summary>
    /// Creates a new row with the given values.
    /// </summary>
    public Row(TableSchema schema, DataValue[] values)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        Values = values ?? throw new ArgumentNullException(nameof(values));

        if (values.Length != schema.Columns.Count)
        {
            throw new ArgumentException(
                $"Row has {values.Length} values but schema expects {schema.Columns.Count}");
        }
    }

    /// <summary>
    /// Gets a value by column name.
    /// </summary>
    public DataValue GetValue(string columnName)
    {
        var ordinal = Schema.GetColumnOrdinal(columnName);
        if (ordinal < 0)
            throw new ArgumentException($"Column not found: {columnName}");

        return Values[ordinal];
    }

    /// <summary>
    /// Gets a value by column ordinal.
    /// </summary>
    public DataValue GetValue(int ordinal)
    {
        if (ordinal < 0 || ordinal >= Values.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal));

        return Values[ordinal];
    }

    /// <summary>
    /// Sets a value by column ordinal.
    /// </summary>
    public void SetValue(int ordinal, DataValue value)
    {
        if (ordinal < 0 || ordinal >= Values.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal));

        Values[ordinal] = value;
    }

    /// <summary>
    /// Sets a value by column name.
    /// </summary>
    public void SetValue(string columnName, DataValue value)
    {
        var ordinal = Schema.GetColumnOrdinal(columnName);
        if (ordinal < 0)
            throw new ArgumentException($"Column not found: {columnName}");

        Values[ordinal] = value;
    }

    /// <summary>
    /// Serializes this row to bytes for storage.
    /// Format: [null bitmap][value1][value2]...
    /// Variable-length values include length prefixes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // Write null bitmap
        var nullBitmap = new byte[(Values.Length + 7) / 8];
        for (int i = 0; i < Values.Length; i++)
        {
            if (Values[i].IsNull)
            {
                nullBitmap[i / 8] |= (byte)(1 << (i % 8));
            }
        }
        writer.Write(nullBitmap);

        // Write values
        for (int i = 0; i < Values.Length; i++)
        {
            if (!Values[i].IsNull)
            {
                var valueBytes = Values[i].SerializeValue();
                writer.Write(valueBytes);
            }
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a row from bytes.
    /// </summary>
    public static Row Deserialize(byte[] data, TableSchema schema)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var columnCount = schema.Columns.Count;

        // Read null bitmap
        var nullBitmapSize = (columnCount + 7) / 8;
        var nullBitmap = reader.ReadBytes(nullBitmapSize);

        // Read values
        var values = new DataValue[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
            var isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
            if (isNull)
            {
                values[i] = DataValue.Null;
            }
            else
            {
                values[i] = DataValue.DeserializeValue(reader, schema.Columns[i].DataType);
            }
        }

        return new Row(schema, values);
    }

    /// <summary>
    /// Creates a copy of this row.
    /// </summary>
    public Row Clone()
    {
        var newValues = new DataValue[Values.Length];
        Array.Copy(Values, newValues, Values.Length);
        return new Row(Schema, newValues)
        {
            RowId = RowId
        };
    }

    public override string ToString()
    {
        var values = string.Join(", ", Values.Select(v => v.ToString()));
        return $"Row({values})";
    }
}

/// <summary>
/// Identifies a row by its page and slot.
/// </summary>
public readonly struct RowId : IEquatable<RowId>
{
    /// <summary>
    /// The page ID containing this row.
    /// </summary>
    public int PageId { get; }

    /// <summary>
    /// The slot number within the page.
    /// </summary>
    public short SlotNumber { get; }

    /// <summary>
    /// An invalid row ID.
    /// </summary>
    public static RowId Invalid => new(-1, -1);

    /// <summary>
    /// Whether this is a valid row ID.
    /// </summary>
    public bool IsValid => PageId >= 0 && SlotNumber >= 0;

    public RowId(int pageId, short slotNumber)
    {
        PageId = pageId;
        SlotNumber = slotNumber;
    }

    public bool Equals(RowId other) => PageId == other.PageId && SlotNumber == other.SlotNumber;
    public override bool Equals(object? obj) => obj is RowId other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(PageId, SlotNumber);

    public static bool operator ==(RowId left, RowId right) => left.Equals(right);
    public static bool operator !=(RowId left, RowId right) => !left.Equals(right);

    public override string ToString() => $"RowId({PageId}:{SlotNumber})";
}
