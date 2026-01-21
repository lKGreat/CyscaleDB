using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents a row of data in a table.
/// Row Header Format (InnoDB-style):
/// +------------+------------+------------+
/// | TRX_ID(8B) | ROLL_PTR(8B)| ROW_DATA  |
/// +------------+------------+------------+
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
    /// The transaction ID that last modified this row (TRX_ID).
    /// Used for MVCC visibility checks.
    /// </summary>
    public long TransactionId { get; set; }

    /// <summary>
    /// Pointer to the undo log record for the previous version (ROLL_PTR).
    /// Used to construct the version chain for MVCC.
    /// Format: combines undo log segment, page number, and offset.
    /// </summary>
    public long RollPointer { get; set; }

    /// <summary>
    /// Whether this row has been deleted (but not yet purged).
    /// In MVCC, deleted rows are marked but kept for snapshot reads.
    /// </summary>
    public bool IsDeleted { get; set; }

    /// <summary>
    /// Set of column ordinals that should be lazily filled with default values.
    /// This is used for online ADD COLUMN operations where old rows don't have the new column yet.
    /// </summary>
    private HashSet<int>? _lazyColumns;

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
        
        // Initialize MVCC fields with default values
        TransactionId = 0;
        RollPointer = RollPointerHelper.Invalid;
        IsDeleted = false;
    }

    /// <summary>
    /// Creates a new row with MVCC metadata.
    /// </summary>
    public Row(TableSchema schema, DataValue[] values, long transactionId, long rollPointer = 0, bool isDeleted = false)
        : this(schema, values)
    {
        TransactionId = transactionId;
        RollPointer = rollPointer;
        IsDeleted = isDeleted;
    }

    /// <summary>
    /// Gets a value by column name.
    /// </summary>
    public DataValue GetValue(string columnName)
    {
        var ordinal = Schema.GetColumnOrdinal(columnName);
        if (ordinal < 0)
            throw new ArgumentException($"Column not found: {columnName}");

        return GetValue(ordinal);
    }

    /// <summary>
    /// Gets a value by column ordinal.
    /// For lazy columns (added via online DDL), returns the default value if not yet filled.
    /// </summary>
    public DataValue GetValue(int ordinal)
    {
        if (ordinal < 0 || ordinal >= Values.Length)
            throw new ArgumentOutOfRangeException(nameof(ordinal));

        // Check if this is a lazy column that needs default value
        if (_lazyColumns?.Contains(ordinal) == true)
        {
            var column = Schema.Columns[ordinal];
            return column.DefaultValue ?? DataValue.Null;
        }

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
    /// Format: [TRX_ID(8)][ROLL_PTR(8)][FLAGS(1)][null bitmap][value1][value2]...
    /// Variable-length values include length prefixes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // Write MVCC header
        writer.Write(TransactionId);
        writer.Write(RollPointer);
        
        // Write flags (currently just IsDeleted)
        byte flags = 0;
        if (IsDeleted) flags |= 0x01;
        writer.Write(flags);

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
    /// Serializes this row without MVCC metadata (for backward compatibility).
    /// </summary>
    public byte[] SerializeWithoutMvcc()
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
    /// Deserializes a row from bytes (with MVCC metadata).
    /// </summary>
    public static Row Deserialize(byte[] data, TableSchema schema)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        // Read MVCC header
        var transactionId = reader.ReadInt64();
        var rollPointer = reader.ReadInt64();
        var flags = reader.ReadByte();
        var isDeleted = (flags & 0x01) != 0;

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

        return new Row(schema, values, transactionId, rollPointer, isDeleted);
    }

    /// <summary>
    /// Deserializes a row from bytes without MVCC metadata (for backward compatibility).
    /// </summary>
    public static Row DeserializeWithoutMvcc(byte[] data, TableSchema schema)
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
        var newRow = new Row(Schema, newValues, TransactionId, RollPointer, IsDeleted)
        {
            RowId = RowId
        };
        
        // Copy lazy column info
        if (_lazyColumns != null)
        {
            newRow._lazyColumns = new HashSet<int>(_lazyColumns);
        }
        
        return newRow;
    }

    /// <summary>
    /// Creates a copy of this row with a new transaction ID (for updates).
    /// </summary>
    public Row CloneWithNewTransaction(long newTransactionId, long newRollPointer)
    {
        var newValues = new DataValue[Values.Length];
        Array.Copy(Values, newValues, Values.Length);
        var newRow = new Row(Schema, newValues, newTransactionId, newRollPointer, IsDeleted)
        {
            RowId = RowId
        };
        
        // Copy lazy column info
        if (_lazyColumns != null)
        {
            newRow._lazyColumns = new HashSet<int>(_lazyColumns);
        }
        
        return newRow;
    }

    /// <summary>
    /// Marks a column as lazy (will return default value until backfilled).
    /// Used for online ADD COLUMN operations.
    /// </summary>
    public void MarkColumnAsLazy(int columnOrdinal)
    {
        _lazyColumns ??= new HashSet<int>();
        _lazyColumns.Add(columnOrdinal);
    }

    /// <summary>
    /// Backfills a lazy column with its actual value.
    /// </summary>
    public void BackfillColumn(int columnOrdinal, DataValue value)
    {
        if (_lazyColumns?.Remove(columnOrdinal) == true)
        {
            Values[columnOrdinal] = value;
            
            // Clean up the set if empty
            if (_lazyColumns.Count == 0)
            {
                _lazyColumns = null;
            }
        }
    }

    /// <summary>
    /// Checks if a column is lazy (not yet backfilled).
    /// </summary>
    public bool IsColumnLazy(int columnOrdinal)
    {
        return _lazyColumns?.Contains(columnOrdinal) == true;
    }

    /// <summary>
    /// Gets the number of lazy columns in this row.
    /// </summary>
    public int LazyColumnCount => _lazyColumns?.Count ?? 0;

    public override string ToString()
    {
        var values = string.Join(", ", Values.Select(v => v.ToString()));
        var lazyInfo = _lazyColumns != null && _lazyColumns.Count > 0 
            ? $", LazyColumns={_lazyColumns.Count}" 
            : "";
        return $"Row(TrxId={TransactionId}, RollPtr={RollPointer}, Deleted={IsDeleted}{lazyInfo}, Values=[{values}])";
    }
}

/// <summary>
/// Helper class for encoding/decoding roll pointers.
/// Roll pointer format (7 bytes in InnoDB, we use 8 for simplicity):
/// [SegmentId(2)][PageNo(4)][Offset(2)]
/// </summary>
public static class RollPointerHelper
{
    /// <summary>
    /// Invalid roll pointer value (no previous version).
    /// </summary>
    public const long Invalid = 0;

    /// <summary>
    /// Creates a roll pointer from its components.
    /// </summary>
    public static long Create(ushort segmentId, uint pageNumber, ushort offset)
    {
        return ((long)segmentId << 48) | ((long)pageNumber << 16) | offset;
    }

    /// <summary>
    /// Extracts the segment ID from a roll pointer.
    /// </summary>
    public static ushort GetSegmentId(long rollPointer)
    {
        return (ushort)(rollPointer >> 48);
    }

    /// <summary>
    /// Extracts the page number from a roll pointer.
    /// </summary>
    public static uint GetPageNumber(long rollPointer)
    {
        return (uint)((rollPointer >> 16) & 0xFFFFFFFF);
    }

    /// <summary>
    /// Extracts the offset from a roll pointer.
    /// </summary>
    public static ushort GetOffset(long rollPointer)
    {
        return (ushort)(rollPointer & 0xFFFF);
    }

    /// <summary>
    /// Checks if a roll pointer is valid (points to an undo record).
    /// </summary>
    public static bool IsValid(long rollPointer)
    {
        return rollPointer != Invalid;
    }
}

/// <summary>
/// Identifies a row by its page and slot.
/// </summary>
public readonly struct RowId : IEquatable<RowId>, IComparable<RowId>
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

    public int CompareTo(RowId other)
    {
        var cmp = PageId.CompareTo(other.PageId);
        return cmp != 0 ? cmp : SlotNumber.CompareTo(other.SlotNumber);
    }

    public static bool operator ==(RowId left, RowId right) => left.Equals(right);
    public static bool operator !=(RowId left, RowId right) => !left.Equals(right);

    public override string ToString() => $"RowId({PageId}:{SlotNumber})";

    /// <summary>
    /// Serializes this RowId to bytes (6 bytes: 4 for PageId + 2 for SlotNumber).
    /// </summary>
    public byte[] Serialize()
    {
        var bytes = new byte[6];
        BitConverter.TryWriteBytes(bytes.AsSpan(0, 4), PageId);
        BitConverter.TryWriteBytes(bytes.AsSpan(4, 2), SlotNumber);
        return bytes;
    }

    /// <summary>
    /// Deserializes a RowId from bytes.
    /// </summary>
    public static RowId Deserialize(ReadOnlySpan<byte> data)
    {
        var pageId = BitConverter.ToInt32(data[..4]);
        var slotNumber = BitConverter.ToInt16(data.Slice(4, 2));
        return new RowId(pageId, slotNumber);
    }
}
