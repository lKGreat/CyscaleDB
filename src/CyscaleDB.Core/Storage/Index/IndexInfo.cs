using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage.Index;

/// <summary>
/// Types of indexes supported by CyscaleDB.
/// </summary>
public enum IndexType : byte
{
    /// <summary>
    /// B-Tree index for range queries and ordered access.
    /// </summary>
    BTree = 0,

    /// <summary>
    /// Hash index for equality lookups.
    /// </summary>
    Hash = 1
}

/// <summary>
/// Represents metadata about an index.
/// </summary>
public sealed class IndexInfo
{
    /// <summary>
    /// Unique identifier for this index.
    /// </summary>
    public int IndexId { get; }

    /// <summary>
    /// The name of this index.
    /// </summary>
    public string IndexName { get; }

    /// <summary>
    /// The name of the table this index belongs to.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The name of the database containing this index.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// The type of this index (BTree or Hash).
    /// </summary>
    public IndexType Type { get; }

    /// <summary>
    /// The columns included in this index, in order.
    /// </summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>
    /// Column ordinal positions in the table schema.
    /// </summary>
    public IReadOnlyList<int> ColumnOrdinals { get; private set; }

    /// <summary>
    /// Whether this is a unique index.
    /// </summary>
    public bool IsUnique { get; }

    /// <summary>
    /// Whether this is the primary key index.
    /// </summary>
    public bool IsPrimaryKey { get; }

    /// <summary>
    /// When this index was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// The file path for this index data.
    /// </summary>
    public string FilePath { get; private set; } = string.Empty;

    /// <summary>
    /// Creates a new IndexInfo instance.
    /// </summary>
    public IndexInfo(
        int indexId,
        string indexName,
        string tableName,
        string databaseName,
        IndexType type,
        IEnumerable<string> columns,
        bool isUnique = false,
        bool isPrimaryKey = false,
        DateTime? createdAt = null)
    {
        if (string.IsNullOrWhiteSpace(indexName))
            throw new ArgumentException("Index name cannot be empty", nameof(indexName));
        if (indexName.Length > Constants.MaxIndexNameLength)
            throw new ArgumentException($"Index name exceeds maximum length of {Constants.MaxIndexNameLength}", nameof(indexName));
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be empty", nameof(tableName));

        var columnList = columns.ToList();
        if (columnList.Count == 0)
            throw new ArgumentException("Index must have at least one column", nameof(columns));

        IndexId = indexId;
        IndexName = indexName;
        TableName = tableName;
        DatabaseName = databaseName;
        Type = type;
        Columns = columnList.AsReadOnly();
        ColumnOrdinals = [];
        IsUnique = isUnique || isPrimaryKey; // Primary key implies unique
        IsPrimaryKey = isPrimaryKey;
        CreatedAt = createdAt ?? DateTime.UtcNow;
    }

    /// <summary>
    /// Sets the column ordinals based on the table schema.
    /// </summary>
    public void ResolveColumnOrdinals(TableSchema schema)
    {
        var ordinals = new List<int>();
        foreach (var colName in Columns)
        {
            var ordinal = schema.GetColumnOrdinal(colName);
            if (ordinal < 0)
                throw new ColumnNotFoundException(colName, TableName);
            ordinals.Add(ordinal);
        }
        ColumnOrdinals = ordinals.AsReadOnly();
    }

    /// <summary>
    /// Sets the file path for this index.
    /// </summary>
    public void SetFilePath(string path)
    {
        FilePath = path;
    }

    /// <summary>
    /// Gets the index file extension based on the index type.
    /// </summary>
    public string GetFileExtension()
    {
        return Type switch
        {
            IndexType.BTree => Constants.IndexFileExtension,
            IndexType.Hash => Constants.HashIndexExtension,
            _ => Constants.IndexFileExtension
        };
    }

    /// <summary>
    /// Extracts key values from a row for this index.
    /// </summary>
    public DataValue[] ExtractKeyValues(Row row)
    {
        var keys = new DataValue[ColumnOrdinals.Count];
        for (int i = 0; i < ColumnOrdinals.Count; i++)
        {
            keys[i] = row.Values[ColumnOrdinals[i]];
        }
        return keys;
    }

    /// <summary>
    /// Serializes this index info to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(IndexId);
        writer.Write(IndexName);
        writer.Write(TableName);
        writer.Write(DatabaseName);
        writer.Write((byte)Type);
        writer.Write(IsUnique);
        writer.Write(IsPrimaryKey);
        writer.Write(CreatedAt.Ticks);

        // Columns
        writer.Write(Columns.Count);
        foreach (var col in Columns)
        {
            writer.Write(col);
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes an IndexInfo from bytes.
    /// </summary>
    public static IndexInfo Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var indexId = reader.ReadInt32();
        var indexName = reader.ReadString();
        var tableName = reader.ReadString();
        var databaseName = reader.ReadString();
        var type = (IndexType)reader.ReadByte();
        var isUnique = reader.ReadBoolean();
        var isPrimaryKey = reader.ReadBoolean();
        var createdAt = new DateTime(reader.ReadInt64());

        var columnCount = reader.ReadInt32();
        var columns = new List<string>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            columns.Add(reader.ReadString());
        }

        return new IndexInfo(indexId, indexName, tableName, databaseName, type, columns, isUnique, isPrimaryKey, createdAt);
    }

    /// <summary>
    /// Creates a composite key from multiple DataValues.
    /// </summary>
    public static CompositeKey CreateCompositeKey(DataValue[] values)
    {
        return new CompositeKey(values);
    }

    public override string ToString() =>
        $"Index '{IndexName}' on {DatabaseName}.{TableName}({string.Join(", ", Columns)}) [{Type}]";
}

/// <summary>
/// Represents a composite key for multi-column indexes.
/// </summary>
public readonly struct CompositeKey : IComparable<CompositeKey>, IEquatable<CompositeKey>
{
    private readonly DataValue[] _values;

    /// <summary>
    /// Gets the key values.
    /// </summary>
    public ReadOnlySpan<DataValue> Values => _values;

    /// <summary>
    /// Gets the number of values in this key.
    /// </summary>
    public int Length => _values?.Length ?? 0;

    public CompositeKey(DataValue[] values)
    {
        _values = values ?? [];
    }

    public int CompareTo(CompositeKey other)
    {
        var minLen = Math.Min(Length, other.Length);
        for (int i = 0; i < minLen; i++)
        {
            var cmp = _values[i].CompareTo(other._values[i]);
            if (cmp != 0)
                return cmp;
        }
        return Length.CompareTo(other.Length);
    }

    public bool Equals(CompositeKey other)
    {
        if (Length != other.Length)
            return false;
        for (int i = 0; i < Length; i++)
        {
            if (!_values[i].Equals(other._values[i]))
                return false;
        }
        return true;
    }

    public override bool Equals(object? obj) => obj is CompositeKey other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var v in _values)
        {
            hash.Add(v);
        }
        return hash.ToHashCode();
    }

    public static bool operator ==(CompositeKey left, CompositeKey right) => left.Equals(right);
    public static bool operator !=(CompositeKey left, CompositeKey right) => !left.Equals(right);
    public static bool operator <(CompositeKey left, CompositeKey right) => left.CompareTo(right) < 0;
    public static bool operator <=(CompositeKey left, CompositeKey right) => left.CompareTo(right) <= 0;
    public static bool operator >(CompositeKey left, CompositeKey right) => left.CompareTo(right) > 0;
    public static bool operator >=(CompositeKey left, CompositeKey right) => left.CompareTo(right) >= 0;

    public override string ToString()
    {
        return _values == null ? "(null)" : $"({string.Join(", ", _values.Select(v => v.ToString()))})";
    }
}

