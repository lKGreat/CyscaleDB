using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Defines the action to take when referenced data is updated or deleted.
/// </summary>
public enum ForeignKeyAction
{
    /// <summary>
    /// Reject the operation (default behavior).
    /// </summary>
    Restrict = 0,

    /// <summary>
    /// No action (similar to Restrict but checked at end of transaction).
    /// </summary>
    NoAction = 1,

    /// <summary>
    /// Cascade the operation to referencing rows.
    /// </summary>
    Cascade = 2,

    /// <summary>
    /// Set referencing columns to NULL.
    /// </summary>
    SetNull = 3,

    /// <summary>
    /// Set referencing columns to their default values.
    /// </summary>
    SetDefault = 4
}

/// <summary>
/// Represents a foreign key constraint definition.
/// </summary>
public sealed class ForeignKeyInfo
{
    /// <summary>
    /// Unique identifier for this foreign key.
    /// </summary>
    public int ForeignKeyId { get; }

    /// <summary>
    /// The name of this foreign key constraint.
    /// </summary>
    public string ConstraintName { get; }

    /// <summary>
    /// The database containing the referencing table.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// The table containing the foreign key columns.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The columns in this table that make up the foreign key.
    /// </summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>
    /// The database containing the referenced table.
    /// </summary>
    public string ReferencedDatabaseName { get; }

    /// <summary>
    /// The table being referenced.
    /// </summary>
    public string ReferencedTableName { get; }

    /// <summary>
    /// The columns in the referenced table.
    /// </summary>
    public IReadOnlyList<string> ReferencedColumns { get; }

    /// <summary>
    /// Action to take when referenced row is deleted.
    /// </summary>
    public ForeignKeyAction OnDelete { get; }

    /// <summary>
    /// Action to take when referenced row is updated.
    /// </summary>
    public ForeignKeyAction OnUpdate { get; }

    /// <summary>
    /// When this foreign key was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Creates a new ForeignKeyInfo instance.
    /// </summary>
    public ForeignKeyInfo(
        int foreignKeyId,
        string constraintName,
        string databaseName,
        string tableName,
        IEnumerable<string> columns,
        string referencedDatabaseName,
        string referencedTableName,
        IEnumerable<string> referencedColumns,
        ForeignKeyAction onDelete = ForeignKeyAction.Restrict,
        ForeignKeyAction onUpdate = ForeignKeyAction.Restrict,
        DateTime? createdAt = null)
    {
        if (string.IsNullOrWhiteSpace(constraintName))
            throw new ArgumentException("Constraint name cannot be empty", nameof(constraintName));

        var columnList = columns.ToList();
        var refColumnList = referencedColumns.ToList();

        if (columnList.Count == 0)
            throw new ArgumentException("Foreign key must have at least one column", nameof(columns));
        if (columnList.Count != refColumnList.Count)
            throw new ArgumentException("Column count must match referenced column count");

        ForeignKeyId = foreignKeyId;
        ConstraintName = constraintName;
        DatabaseName = databaseName;
        TableName = tableName;
        Columns = columnList.AsReadOnly();
        ReferencedDatabaseName = referencedDatabaseName;
        ReferencedTableName = referencedTableName;
        ReferencedColumns = refColumnList.AsReadOnly();
        OnDelete = onDelete;
        OnUpdate = onUpdate;
        CreatedAt = createdAt ?? DateTime.UtcNow;
    }

    /// <summary>
    /// Gets the full qualified name of the referencing table.
    /// </summary>
    public string GetTableQualifiedName() => $"{DatabaseName}.{TableName}";

    /// <summary>
    /// Gets the full qualified name of the referenced table.
    /// </summary>
    public string GetReferencedTableQualifiedName() => $"{ReferencedDatabaseName}.{ReferencedTableName}";

    /// <summary>
    /// Serializes this foreign key info to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(ForeignKeyId);
        writer.Write(ConstraintName);
        writer.Write(DatabaseName);
        writer.Write(TableName);

        writer.Write(Columns.Count);
        foreach (var col in Columns)
        {
            writer.Write(col);
        }

        writer.Write(ReferencedDatabaseName);
        writer.Write(ReferencedTableName);

        writer.Write(ReferencedColumns.Count);
        foreach (var col in ReferencedColumns)
        {
            writer.Write(col);
        }

        writer.Write((byte)OnDelete);
        writer.Write((byte)OnUpdate);
        writer.Write(CreatedAt.Ticks);

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a ForeignKeyInfo from bytes.
    /// </summary>
    public static ForeignKeyInfo Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var foreignKeyId = reader.ReadInt32();
        var constraintName = reader.ReadString();
        var databaseName = reader.ReadString();
        var tableName = reader.ReadString();

        var columnCount = reader.ReadInt32();
        var columns = new List<string>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            columns.Add(reader.ReadString());
        }

        var refDatabaseName = reader.ReadString();
        var refTableName = reader.ReadString();

        var refColumnCount = reader.ReadInt32();
        var refColumns = new List<string>(refColumnCount);
        for (int i = 0; i < refColumnCount; i++)
        {
            refColumns.Add(reader.ReadString());
        }

        var onDelete = (ForeignKeyAction)reader.ReadByte();
        var onUpdate = (ForeignKeyAction)reader.ReadByte();
        var createdAt = new DateTime(reader.ReadInt64());

        return new ForeignKeyInfo(
            foreignKeyId, constraintName, databaseName, tableName, columns,
            refDatabaseName, refTableName, refColumns,
            onDelete, onUpdate, createdAt);
    }

    public override string ToString()
    {
        return $"FK '{ConstraintName}' on {GetTableQualifiedName()}({string.Join(", ", Columns)}) " +
               $"-> {GetReferencedTableQualifiedName()}({string.Join(", ", ReferencedColumns)})";
    }
}

/// <summary>
/// Manages foreign key constraints.
/// </summary>
public sealed class ForeignKeyManager
{
    private readonly Dictionary<string, List<ForeignKeyInfo>> _fksByTable = [];
    private readonly Dictionary<string, List<ForeignKeyInfo>> _fksByReferencedTable = [];
    private readonly Dictionary<string, ForeignKeyInfo> _fksByName = [];
    private readonly object _lock = new();
    private readonly Logger _logger;
    private int _nextForeignKeyId;

    public ForeignKeyManager()
    {
        _logger = LogManager.Default.GetLogger<ForeignKeyManager>();
        _nextForeignKeyId = 1;
    }

    /// <summary>
    /// Adds a new foreign key constraint.
    /// </summary>
    public ForeignKeyInfo AddForeignKey(
        string constraintName,
        string databaseName,
        string tableName,
        IEnumerable<string> columns,
        string referencedDatabaseName,
        string referencedTableName,
        IEnumerable<string> referencedColumns,
        ForeignKeyAction onDelete = ForeignKeyAction.Restrict,
        ForeignKeyAction onUpdate = ForeignKeyAction.Restrict)
    {
        lock (_lock)
        {
            var fullName = $"{databaseName}.{constraintName}";

            if (_fksByName.ContainsKey(fullName))
            {
                throw new ConstraintViolationException(
                    $"Foreign key constraint '{constraintName}' already exists",
                    constraintName);
            }

            var fk = new ForeignKeyInfo(
                _nextForeignKeyId++,
                constraintName,
                databaseName,
                tableName,
                columns,
                referencedDatabaseName,
                referencedTableName,
                referencedColumns,
                onDelete,
                onUpdate);

            _fksByName[fullName] = fk;

            var tableKey = fk.GetTableQualifiedName();
            if (!_fksByTable.TryGetValue(tableKey, out var tableFks))
            {
                tableFks = [];
                _fksByTable[tableKey] = tableFks;
            }
            tableFks.Add(fk);

            var refTableKey = fk.GetReferencedTableQualifiedName();
            if (!_fksByReferencedTable.TryGetValue(refTableKey, out var refTableFks))
            {
                refTableFks = [];
                _fksByReferencedTable[refTableKey] = refTableFks;
            }
            refTableFks.Add(fk);

            _logger.Info("Added foreign key: {0}", fk);
            return fk;
        }
    }

    /// <summary>
    /// Drops a foreign key constraint.
    /// </summary>
    public bool DropForeignKey(string databaseName, string constraintName)
    {
        lock (_lock)
        {
            var fullName = $"{databaseName}.{constraintName}";

            if (!_fksByName.TryGetValue(fullName, out var fk))
            {
                return false;
            }

            _fksByName.Remove(fullName);

            var tableKey = fk.GetTableQualifiedName();
            if (_fksByTable.TryGetValue(tableKey, out var tableFks))
            {
                tableFks.Remove(fk);
                if (tableFks.Count == 0)
                    _fksByTable.Remove(tableKey);
            }

            var refTableKey = fk.GetReferencedTableQualifiedName();
            if (_fksByReferencedTable.TryGetValue(refTableKey, out var refTableFks))
            {
                refTableFks.Remove(fk);
                if (refTableFks.Count == 0)
                    _fksByReferencedTable.Remove(refTableKey);
            }

            _logger.Info("Dropped foreign key: {0}", fk);
            return true;
        }
    }

    /// <summary>
    /// Gets all foreign keys on a table.
    /// </summary>
    public IReadOnlyList<ForeignKeyInfo> GetForeignKeysOnTable(string databaseName, string tableName)
    {
        lock (_lock)
        {
            var tableKey = $"{databaseName}.{tableName}";
            if (_fksByTable.TryGetValue(tableKey, out var fks))
            {
                return fks.AsReadOnly();
            }
            return [];
        }
    }

    /// <summary>
    /// Gets all foreign keys referencing a table.
    /// </summary>
    public IReadOnlyList<ForeignKeyInfo> GetForeignKeysReferencingTable(string databaseName, string tableName)
    {
        lock (_lock)
        {
            var tableKey = $"{databaseName}.{tableName}";
            if (_fksByReferencedTable.TryGetValue(tableKey, out var fks))
            {
                return fks.AsReadOnly();
            }
            return [];
        }
    }

    /// <summary>
    /// Gets a foreign key by name.
    /// </summary>
    public ForeignKeyInfo? GetForeignKey(string databaseName, string constraintName)
    {
        lock (_lock)
        {
            var fullName = $"{databaseName}.{constraintName}";
            return _fksByName.GetValueOrDefault(fullName);
        }
    }

    /// <summary>
    /// Validates that an insert or update would not violate any foreign key constraints.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="row">The row being inserted/updated</param>
    /// <param name="schema">Table schema</param>
    /// <param name="referencedRowExists">Function to check if a referenced row exists</param>
    public void ValidateInsert(
        string databaseName,
        string tableName,
        Row row,
        TableSchema schema,
        Func<string, string, DataValue[], bool> referencedRowExists)
    {
        var fks = GetForeignKeysOnTable(databaseName, tableName);

        foreach (var fk in fks)
        {
            var fkValues = new DataValue[fk.Columns.Count];
            bool hasNullValue = false;

            for (int i = 0; i < fk.Columns.Count; i++)
            {
                var colIndex = schema.GetColumnOrdinal(fk.Columns[i]);
                if (colIndex < 0)
                    throw new ColumnNotFoundException(fk.Columns[i], tableName);

                fkValues[i] = row.Values[colIndex];
                if (fkValues[i].IsNull)
                {
                    hasNullValue = true;
                    break;
                }
            }

            // NULL values are always allowed in FK columns (match nothing)
            if (hasNullValue)
                continue;

            // Check that referenced row exists
            if (!referencedRowExists(fk.ReferencedDatabaseName, fk.ReferencedTableName, fkValues))
            {
                throw new ConstraintViolationException(
                    $"Cannot add or update row: foreign key constraint '{fk.ConstraintName}' fails",
                    fk.ConstraintName);
            }
        }
    }

    /// <summary>
    /// Checks if a delete or update would violate any foreign key constraints referencing this table.
    /// </summary>
    /// <param name="databaseName">Database name</param>
    /// <param name="tableName">Table name</param>
    /// <param name="row">The row being deleted/updated</param>
    /// <param name="schema">Table schema</param>
    /// <param name="referencingRowsExist">Function to check if referencing rows exist</param>
    /// <returns>List of FK actions that need to be cascaded</returns>
    public List<(ForeignKeyInfo Fk, ForeignKeyAction Action)> ValidateDeleteOrUpdate(
        string databaseName,
        string tableName,
        Row row,
        TableSchema schema,
        Func<string, string, IReadOnlyList<string>, DataValue[], bool> referencingRowsExist,
        bool isDelete)
    {
        var cascadeActions = new List<(ForeignKeyInfo, ForeignKeyAction)>();
        var fks = GetForeignKeysReferencingTable(databaseName, tableName);

        foreach (var fk in fks)
        {
            var refValues = new DataValue[fk.ReferencedColumns.Count];

            for (int i = 0; i < fk.ReferencedColumns.Count; i++)
            {
                var colIndex = schema.GetColumnOrdinal(fk.ReferencedColumns[i]);
                if (colIndex < 0)
                    throw new ColumnNotFoundException(fk.ReferencedColumns[i], tableName);

                refValues[i] = row.Values[colIndex];
            }

            // Check if any rows reference this one
            if (referencingRowsExist(fk.DatabaseName, fk.TableName, fk.Columns, refValues))
            {
                var action = isDelete ? fk.OnDelete : fk.OnUpdate;

                switch (action)
                {
                    case ForeignKeyAction.Restrict:
                    case ForeignKeyAction.NoAction:
                        throw new ConstraintViolationException(
                            $"Cannot delete or update parent row: foreign key constraint '{fk.ConstraintName}' fails",
                            fk.ConstraintName);

                    case ForeignKeyAction.Cascade:
                    case ForeignKeyAction.SetNull:
                    case ForeignKeyAction.SetDefault:
                        cascadeActions.Add((fk, action));
                        break;
                }
            }
        }

        return cascadeActions;
    }

    /// <summary>
    /// Drops all foreign keys referencing a table (when table is being dropped).
    /// </summary>
    public void DropForeignKeysReferencingTable(string databaseName, string tableName)
    {
        lock (_lock)
        {
            var tableKey = $"{databaseName}.{tableName}";
            if (!_fksByReferencedTable.TryGetValue(tableKey, out var fks))
                return;

            foreach (var fk in fks.ToList())
            {
                DropForeignKey(fk.DatabaseName, fk.ConstraintName);
            }
        }
    }

    /// <summary>
    /// Drops all foreign keys on a table (when table is being dropped).
    /// </summary>
    public void DropForeignKeysOnTable(string databaseName, string tableName)
    {
        lock (_lock)
        {
            var tableKey = $"{databaseName}.{tableName}";
            if (!_fksByTable.TryGetValue(tableKey, out var fks))
                return;

            foreach (var fk in fks.ToList())
            {
                DropForeignKey(fk.DatabaseName, fk.ConstraintName);
            }
        }
    }

    /// <summary>
    /// Gets all foreign keys in the system.
    /// </summary>
    public IReadOnlyList<ForeignKeyInfo> GetAllForeignKeys()
    {
        lock (_lock)
        {
            return _fksByName.Values.ToList().AsReadOnly();
        }
    }
}
