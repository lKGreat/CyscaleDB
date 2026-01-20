using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents metadata about a database.
/// </summary>
public sealed class DatabaseInfo
{
    /// <summary>
    /// Unique identifier for this database.
    /// </summary>
    public int DatabaseId { get; }

    /// <summary>
    /// The name of this database.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The directory path where this database stores its files.
    /// </summary>
    public string DataDirectory { get; }

    /// <summary>
    /// When this database was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// The tables in this database, keyed by name (case-insensitive).
    /// </summary>
    private readonly Dictionary<string, TableSchema> _tables;

    /// <summary>
    /// Gets all tables in this database.
    /// </summary>
    public IReadOnlyCollection<TableSchema> Tables => _tables.Values;

    /// <summary>
    /// Gets the number of tables in this database.
    /// </summary>
    public int TableCount => _tables.Count;

    /// <summary>
    /// The default character set for this database.
    /// </summary>
    public string CharacterSet { get; }

    /// <summary>
    /// The default collation for this database.
    /// </summary>
    public string Collation { get; }

    /// <summary>
    /// Counter for generating unique table IDs.
    /// </summary>
    private int _nextTableId;

    /// <summary>
    /// Creates a new database info.
    /// </summary>
    public DatabaseInfo(
        int databaseId,
        string name,
        string dataDirectory,
        DateTime? createdAt = null,
        string characterSet = "utf8mb4",
        string collation = "utf8mb4_general_ci")
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Database name cannot be empty", nameof(name));

        if (name.Length > Constants.MaxDatabaseNameLength)
            throw new ArgumentException($"Database name exceeds maximum length of {Constants.MaxDatabaseNameLength}", nameof(name));

        DatabaseId = databaseId;
        Name = name;
        DataDirectory = dataDirectory;
        CreatedAt = createdAt ?? DateTime.UtcNow;
        CharacterSet = characterSet;
        Collation = collation;
        _tables = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
        _nextTableId = 1;
    }

    /// <summary>
    /// Gets a table by name.
    /// </summary>
    public TableSchema? GetTable(string tableName)
    {
        return _tables.TryGetValue(tableName, out var table) ? table : null;
    }

    /// <summary>
    /// Checks if a table exists.
    /// </summary>
    public bool HasTable(string tableName) => _tables.ContainsKey(tableName);

    /// <summary>
    /// Adds a table to this database.
    /// </summary>
    public void AddTable(TableSchema table)
    {
        if (_tables.ContainsKey(table.TableName))
            throw new TableExistsException(table.TableName);

        _tables[table.TableName] = table;
    }

    /// <summary>
    /// Removes a table from this database.
    /// </summary>
    public bool RemoveTable(string tableName)
    {
        return _tables.Remove(tableName);
    }

    /// <summary>
    /// Gets the next unique table ID.
    /// </summary>
    public int GetNextTableId() => _nextTableId++;

    /// <summary>
    /// Sets the next table ID (used during deserialization).
    /// </summary>
    internal void SetNextTableId(int nextId) => _nextTableId = nextId;

    /// <summary>
    /// Serializes this database info to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(DatabaseId);
        writer.Write(Name);
        writer.Write(DataDirectory);
        writer.Write(CreatedAt.Ticks);
        writer.Write(CharacterSet);
        writer.Write(Collation);
        writer.Write(_nextTableId);

        // Write tables
        writer.Write(_tables.Count);
        foreach (var table in _tables.Values)
        {
            var tableBytes = table.Serialize();
            writer.Write(tableBytes.Length);
            writer.Write(tableBytes);
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a database info from bytes.
    /// </summary>
    public static DatabaseInfo Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var databaseId = reader.ReadInt32();
        var name = reader.ReadString();
        var dataDirectory = reader.ReadString();
        var createdAt = new DateTime(reader.ReadInt64());
        var characterSet = reader.ReadString();
        var collation = reader.ReadString();
        var nextTableId = reader.ReadInt32();

        var db = new DatabaseInfo(databaseId, name, dataDirectory, createdAt, characterSet, collation);
        db.SetNextTableId(nextTableId);

        var tableCount = reader.ReadInt32();
        for (int i = 0; i < tableCount; i++)
        {
            var tableLength = reader.ReadInt32();
            var tableBytes = reader.ReadBytes(tableLength);
            var table = TableSchema.Deserialize(tableBytes);
            db._tables[table.TableName] = table;
        }

        return db;
    }

    public override string ToString() => $"Database '{Name}' ({TableCount} tables)";
}
