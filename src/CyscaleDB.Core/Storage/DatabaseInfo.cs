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
    /// The views in this database, keyed by name (case-insensitive).
    /// </summary>
    private readonly Dictionary<string, ViewInfo> _views;

    /// <summary>
    /// The foreign keys in this database, keyed by constraint name.
    /// </summary>
    private readonly Dictionary<string, ForeignKeyDefinition> _foreignKeys;

    /// <summary>
    /// The CHECK constraints in this database, keyed by constraint name.
    /// </summary>
    private readonly Dictionary<string, CheckConstraintDefinition> _checkConstraints;

    /// <summary>
    /// The stored procedures and functions in this database, keyed by name (case-insensitive).
    /// </summary>
    private readonly Dictionary<string, ProcedureInfo> _procedures;

    /// <summary>
    /// Gets all tables in this database.
    /// </summary>
    public IReadOnlyCollection<TableSchema> Tables => _tables.Values;

    /// <summary>
    /// Gets all views in this database.
    /// </summary>
    public IReadOnlyCollection<ViewInfo> Views => _views.Values;

    /// <summary>
    /// Gets all foreign keys in this database.
    /// </summary>
    public IReadOnlyCollection<ForeignKeyDefinition> ForeignKeys => _foreignKeys.Values;

    /// <summary>
    /// Gets all CHECK constraints in this database.
    /// </summary>
    public IReadOnlyCollection<CheckConstraintDefinition> CheckConstraints => _checkConstraints.Values;

    /// <summary>
    /// Gets all stored procedures and functions in this database.
    /// </summary>
    public IReadOnlyCollection<ProcedureInfo> Procedures => _procedures.Values;

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
    /// Counter for generating unique view IDs.
    /// </summary>
    private int _nextViewId;

    /// <summary>
    /// Counter for generating unique procedure IDs.
    /// </summary>
    private int _nextProcedureId;

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
        _views = new Dictionary<string, ViewInfo>(StringComparer.OrdinalIgnoreCase);
        _foreignKeys = new Dictionary<string, ForeignKeyDefinition>(StringComparer.OrdinalIgnoreCase);
        _checkConstraints = new Dictionary<string, CheckConstraintDefinition>(StringComparer.OrdinalIgnoreCase);
        _procedures = new Dictionary<string, ProcedureInfo>(StringComparer.OrdinalIgnoreCase);
        _nextTableId = 1;
        _nextViewId = 1;
        _nextProcedureId = 1;
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

    #region View Management

    /// <summary>
    /// Gets a view by name.
    /// </summary>
    public ViewInfo? GetView(string viewName)
    {
        return _views.TryGetValue(viewName, out var view) ? view : null;
    }

    /// <summary>
    /// Checks if a view exists.
    /// </summary>
    public bool HasView(string viewName) => _views.ContainsKey(viewName);

    /// <summary>
    /// Adds a view to this database.
    /// </summary>
    public void AddView(ViewInfo view)
    {
        if (_views.ContainsKey(view.ViewName))
            throw new CyscaleException($"View '{view.ViewName}' already exists", ErrorCode.ViewExists);

        _views[view.ViewName] = view;
    }

    /// <summary>
    /// Adds or replaces a view in this database.
    /// </summary>
    public void AddOrReplaceView(ViewInfo view)
    {
        _views[view.ViewName] = view;
    }

    /// <summary>
    /// Removes a view from this database.
    /// </summary>
    public bool RemoveView(string viewName)
    {
        return _views.Remove(viewName);
    }

    /// <summary>
    /// Gets the next unique view ID.
    /// </summary>
    public int GetNextViewId() => _nextViewId++;

    /// <summary>
    /// Sets the next view ID (used during deserialization).
    /// </summary>
    internal void SetNextViewId(int nextId) => _nextViewId = nextId;

    #endregion

    #region Foreign Key Management

    /// <summary>
    /// Gets a foreign key by constraint name.
    /// </summary>
    public ForeignKeyDefinition? GetForeignKey(string constraintName)
    {
        return _foreignKeys.TryGetValue(constraintName, out var fk) ? fk : null;
    }

    /// <summary>
    /// Checks if a foreign key exists.
    /// </summary>
    public bool HasForeignKey(string constraintName) => _foreignKeys.ContainsKey(constraintName);

    /// <summary>
    /// Adds a foreign key to this database.
    /// </summary>
    public void AddForeignKey(ForeignKeyDefinition foreignKey)
    {
        if (_foreignKeys.ContainsKey(foreignKey.ConstraintName))
            throw new CyscaleException($"Foreign key constraint '{foreignKey.ConstraintName}' already exists", ErrorCode.ConstraintViolation);

        _foreignKeys[foreignKey.ConstraintName] = foreignKey;
    }

    /// <summary>
    /// Removes a foreign key from this database.
    /// </summary>
    public bool RemoveForeignKey(string constraintName)
    {
        return _foreignKeys.Remove(constraintName);
    }

    /// <summary>
    /// Gets all foreign keys on a specific table.
    /// </summary>
    public IReadOnlyList<ForeignKeyDefinition> GetForeignKeysOnTable(string tableName)
    {
        return _foreignKeys.Values
            .Where(fk => fk.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
            .ToList();
    }

    /// <summary>
    /// Gets all foreign keys referencing a specific table.
    /// </summary>
    public IReadOnlyList<ForeignKeyDefinition> GetForeignKeysReferencingTable(string tableName)
    {
        return _foreignKeys.Values
            .Where(fk => fk.ReferencedTableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
            .ToList();
    }

    /// <summary>
    /// Removes all foreign keys on a table (used when dropping a table).
    /// </summary>
    public void RemoveForeignKeysOnTable(string tableName)
    {
        var keysToRemove = GetForeignKeysOnTable(tableName).Select(fk => fk.ConstraintName).ToList();
        foreach (var key in keysToRemove)
        {
            _foreignKeys.Remove(key);
        }
    }

    #endregion

    #region CHECK Constraint Management

    /// <summary>
    /// Gets a CHECK constraint by constraint name.
    /// </summary>
    public CheckConstraintDefinition? GetCheckConstraint(string constraintName)
    {
        return _checkConstraints.TryGetValue(constraintName, out var chk) ? chk : null;
    }

    /// <summary>
    /// Checks if a CHECK constraint exists.
    /// </summary>
    public bool HasCheckConstraint(string constraintName) => _checkConstraints.ContainsKey(constraintName);

    /// <summary>
    /// Adds a CHECK constraint to this database.
    /// </summary>
    public void AddCheckConstraint(CheckConstraintDefinition checkConstraint)
    {
        if (_checkConstraints.ContainsKey(checkConstraint.ConstraintName))
            throw new CyscaleException($"CHECK constraint '{checkConstraint.ConstraintName}' already exists", ErrorCode.ConstraintViolation);

        _checkConstraints[checkConstraint.ConstraintName] = checkConstraint;
    }

    /// <summary>
    /// Removes a CHECK constraint from this database.
    /// </summary>
    public bool RemoveCheckConstraint(string constraintName)
    {
        return _checkConstraints.Remove(constraintName);
    }

    /// <summary>
    /// Gets all CHECK constraints on a specific table.
    /// </summary>
    public IReadOnlyList<CheckConstraintDefinition> GetCheckConstraintsOnTable(string tableName)
    {
        return _checkConstraints.Values
            .Where(c => c.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
            .ToList();
    }

    /// <summary>
    /// Removes all CHECK constraints on a table (used when dropping a table).
    /// </summary>
    public void RemoveCheckConstraintsOnTable(string tableName)
    {
        var constraintsToRemove = GetCheckConstraintsOnTable(tableName).Select(c => c.ConstraintName).ToList();
        foreach (var name in constraintsToRemove)
        {
            _checkConstraints.Remove(name);
        }
    }

    #endregion

    #region Stored Procedure Management

    /// <summary>
    /// Gets a stored procedure or function by name.
    /// </summary>
    public ProcedureInfo? GetProcedure(string procedureName)
    {
        return _procedures.TryGetValue(procedureName, out var proc) ? proc : null;
    }

    /// <summary>
    /// Checks if a stored procedure or function exists.
    /// </summary>
    public bool HasProcedure(string procedureName) => _procedures.ContainsKey(procedureName);

    /// <summary>
    /// Adds a stored procedure or function to this database.
    /// </summary>
    public void AddProcedure(ProcedureInfo procedure)
    {
        if (_procedures.ContainsKey(procedure.ProcedureName))
            throw new CyscaleException($"Procedure or function '{procedure.ProcedureName}' already exists", ErrorCode.ProcedureExists);

        _procedures[procedure.ProcedureName] = procedure;
    }

    /// <summary>
    /// Adds or replaces a stored procedure or function in this database.
    /// </summary>
    public void AddOrReplaceProcedure(ProcedureInfo procedure)
    {
        _procedures[procedure.ProcedureName] = procedure;
    }

    /// <summary>
    /// Removes a stored procedure or function from this database.
    /// </summary>
    public bool RemoveProcedure(string procedureName)
    {
        return _procedures.Remove(procedureName);
    }

    /// <summary>
    /// Gets the next unique procedure ID.
    /// </summary>
    public int GetNextProcedureId() => _nextProcedureId++;

    /// <summary>
    /// Sets the next procedure ID (used during deserialization).
    /// </summary>
    internal void SetNextProcedureId(int nextId) => _nextProcedureId = nextId;

    #endregion

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
        writer.Write(_nextViewId);
        writer.Write(_nextProcedureId);

        // Write tables
        writer.Write(_tables.Count);
        foreach (var table in _tables.Values)
        {
            var tableBytes = table.Serialize();
            writer.Write(tableBytes.Length);
            writer.Write(tableBytes);
        }

        // Write views
        writer.Write(_views.Count);
        foreach (var view in _views.Values)
        {
            var viewBytes = view.Serialize();
            writer.Write(viewBytes.Length);
            writer.Write(viewBytes);
        }

        // Write foreign keys
        writer.Write(_foreignKeys.Count);
        foreach (var fk in _foreignKeys.Values)
        {
            var fkBytes = fk.Serialize();
            writer.Write(fkBytes.Length);
            writer.Write(fkBytes);
        }

        // Write CHECK constraints
        writer.Write(_checkConstraints.Count);
        foreach (var chk in _checkConstraints.Values)
        {
            var chkBytes = chk.Serialize();
            writer.Write(chkBytes.Length);
            writer.Write(chkBytes);
        }

        // Write stored procedures and functions
        writer.Write(_procedures.Count);
        foreach (var proc in _procedures.Values)
        {
            var procBytes = proc.Serialize();
            writer.Write(procBytes.Length);
            writer.Write(procBytes);
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
        
        // Try to read nextViewId - may not exist in older formats
        var nextViewId = 1;
        if (stream.Position < stream.Length)
        {
            try
            {
                nextViewId = reader.ReadInt32();
            }
            catch
            {
                // Older format without views - reset position and continue
                nextViewId = 1;
            }
        }

        // Try to read nextProcedureId - may not exist in older formats
        var nextProcedureId = 1;
        if (stream.Position < stream.Length)
        {
            try
            {
                nextProcedureId = reader.ReadInt32();
            }
            catch
            {
                // Older format without procedures - reset position and continue
                nextProcedureId = 1;
            }
        }

        var db = new DatabaseInfo(databaseId, name, dataDirectory, createdAt, characterSet, collation);
        db.SetNextTableId(nextTableId);
        db.SetNextViewId(nextViewId);
        db.SetNextProcedureId(nextProcedureId);

        var tableCount = reader.ReadInt32();
        for (int i = 0; i < tableCount; i++)
        {
            var tableLength = reader.ReadInt32();
            var tableBytes = reader.ReadBytes(tableLength);
            var table = TableSchema.Deserialize(tableBytes);
            db._tables[table.TableName] = table;
        }

        // Try to read views - may not exist in older formats
        if (stream.Position < stream.Length)
        {
            try
            {
                var viewCount = reader.ReadInt32();
                for (int i = 0; i < viewCount; i++)
                {
                    var viewLength = reader.ReadInt32();
                    var viewBytes = reader.ReadBytes(viewLength);
                    var view = ViewInfo.Deserialize(viewBytes);
                    db._views[view.ViewName] = view;
                }
            }
            catch
            {
                // Older format without views - ignore
            }
        }

        // Try to read foreign keys - may not exist in older formats
        if (stream.Position < stream.Length)
        {
            try
            {
                var fkCount = reader.ReadInt32();
                for (int i = 0; i < fkCount; i++)
                {
                    var fkLength = reader.ReadInt32();
                    var fkBytes = reader.ReadBytes(fkLength);
                    var fk = ForeignKeyDefinition.Deserialize(fkBytes);
                    db._foreignKeys[fk.ConstraintName] = fk;
                }
            }
            catch
            {
                // Older format without foreign keys - ignore
            }
        }

        // Try to read CHECK constraints - may not exist in older formats
        if (stream.Position < stream.Length)
        {
            try
            {
                var chkCount = reader.ReadInt32();
                for (int i = 0; i < chkCount; i++)
                {
                    var chkLength = reader.ReadInt32();
                    var chkBytes = reader.ReadBytes(chkLength);
                    var chk = CheckConstraintDefinition.Deserialize(chkBytes);
                    db._checkConstraints[chk.ConstraintName] = chk;
                }
            }
            catch
            {
                // Older format without CHECK constraints - ignore
            }
        }

        // Try to read stored procedures - may not exist in older formats
        if (stream.Position < stream.Length)
        {
            try
            {
                var procCount = reader.ReadInt32();
                for (int i = 0; i < procCount; i++)
                {
                    var procLength = reader.ReadInt32();
                    var procBytes = reader.ReadBytes(procLength);
                    var proc = ProcedureInfo.Deserialize(procBytes);
                    db._procedures[proc.ProcedureName] = proc;
                }
            }
            catch
            {
                // Older format without procedures - ignore
            }
        }

        return db;
    }

    public override string ToString() => $"Database '{Name}' ({TableCount} tables)";
}
