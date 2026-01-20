using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents the schema (metadata) of a database table.
/// </summary>
public sealed class TableSchema
{
    /// <summary>
    /// Unique identifier for this table.
    /// </summary>
    public int TableId { get; }

    /// <summary>
    /// The name of the database this table belongs to.
    /// </summary>
    public string DatabaseName { get; }

    /// <summary>
    /// The name of this table.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The fully qualified name (database.table).
    /// </summary>
    public string FullName => $"{DatabaseName}.{TableName}";

    /// <summary>
    /// The columns in this table, ordered by ordinal position.
    /// </summary>
    public IReadOnlyList<ColumnDefinition> Columns => _columns.AsReadOnly();

    /// <summary>
    /// Internal mutable list of columns.
    /// </summary>
    private readonly List<ColumnDefinition> _columns;

    /// <summary>
    /// Quick lookup of column by name (case-insensitive).
    /// </summary>
    private readonly Dictionary<string, ColumnDefinition> _columnsByName;

    /// <summary>
    /// The primary key columns, if any.
    /// </summary>
    public IReadOnlyList<ColumnDefinition> PrimaryKeyColumns { get; }

    /// <summary>
    /// When this table was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// When this table was last modified.
    /// </summary>
    public DateTime ModifiedAt { get; private set; }

    /// <summary>
    /// The current auto-increment value for this table.
    /// </summary>
    public long AutoIncrementValue { get; private set; }

    /// <summary>
    /// The number of rows in this table (approximate, for statistics).
    /// </summary>
    public long RowCount { get; private set; }

    /// <summary>
    /// Creates a new table schema.
    /// </summary>
    public TableSchema(
        int tableId,
        string databaseName,
        string tableName,
        IEnumerable<ColumnDefinition> columns,
        DateTime? createdAt = null)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty", nameof(databaseName));

        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be empty", nameof(tableName));

        if (tableName.Length > Constants.MaxTableNameLength)
            throw new ArgumentException($"Table name exceeds maximum length of {Constants.MaxTableNameLength}", nameof(tableName));

        TableId = tableId;
        DatabaseName = databaseName;
        TableName = tableName;

        var columnList = columns.ToList();
        if (columnList.Count == 0)
            throw new ArgumentException("Table must have at least one column", nameof(columns));

        if (columnList.Count > Constants.MaxColumnsPerTable)
            throw new ArgumentException($"Table exceeds maximum column count of {Constants.MaxColumnsPerTable}", nameof(columns));

        // Assign ordinal positions and validate unique names
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < columnList.Count; i++)
        {
            if (!names.Add(columnList[i].Name))
                throw new ArgumentException($"Duplicate column name: {columnList[i].Name}", nameof(columns));

            columnList[i].OrdinalPosition = i;
        }

        _columns = columnList;
        _columnsByName = columnList.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);
        PrimaryKeyColumns = columnList.Where(c => c.IsPrimaryKey).ToList().AsReadOnly();

        CreatedAt = createdAt ?? DateTime.UtcNow;
        ModifiedAt = CreatedAt;
        AutoIncrementValue = 1;
        RowCount = 0;
    }

    /// <summary>
    /// Gets a column by name (case-insensitive).
    /// </summary>
    public ColumnDefinition? GetColumn(string name)
    {
        return _columnsByName.TryGetValue(name, out var column) ? column : null;
    }

    /// <summary>
    /// Gets a column by ordinal position.
    /// </summary>
    public ColumnDefinition GetColumn(int ordinal)
    {
        if (ordinal < 0 || ordinal >= Columns.Count)
            throw new ArgumentOutOfRangeException(nameof(ordinal));

        return Columns[ordinal];
    }

    /// <summary>
    /// Checks if a column exists.
    /// </summary>
    public bool HasColumn(string name) => _columnsByName.ContainsKey(name);

    /// <summary>
    /// Gets the ordinal position of a column by name.
    /// Returns -1 if not found.
    /// </summary>
    public int GetColumnOrdinal(string name)
    {
        return _columnsByName.TryGetValue(name, out var column) ? column.OrdinalPosition : -1;
    }

    /// <summary>
    /// Gets the next auto-increment value and increments the counter.
    /// </summary>
    public long GetNextAutoIncrementValue()
    {
        return AutoIncrementValue++;
    }

    /// <summary>
    /// Updates the auto-increment value if the given value is higher.
    /// </summary>
    public void UpdateAutoIncrementValue(long value)
    {
        if (value >= AutoIncrementValue)
        {
            AutoIncrementValue = value + 1;
        }
    }

    /// <summary>
    /// Updates the row count.
    /// </summary>
    public void UpdateRowCount(long delta)
    {
        RowCount = Math.Max(0, RowCount + delta);
        ModifiedAt = DateTime.UtcNow;
    }

    #region Online Schema Change Methods

    /// <summary>
    /// Adds a new column to the schema.
    /// For online schema change, this does not modify existing rows.
    /// </summary>
    /// <param name="column">The column definition to add.</param>
    /// <param name="afterColumn">Optional: Insert after this column. If null, adds at end.</param>
    public void AddColumn(ColumnDefinition column, string? afterColumn = null)
    {
        if (_columnsByName.ContainsKey(column.Name))
            throw new ArgumentException($"Column '{column.Name}' already exists", nameof(column));

        if (_columns.Count >= Constants.MaxColumnsPerTable)
            throw new InvalidOperationException($"Cannot add column: table already has maximum {Constants.MaxColumnsPerTable} columns");

        int insertIndex = _columns.Count;

        if (afterColumn != null)
        {
            var afterOrdinal = GetColumnOrdinal(afterColumn);
            if (afterOrdinal < 0)
                throw new ArgumentException($"Column '{afterColumn}' not found", nameof(afterColumn));
            insertIndex = afterOrdinal + 1;
        }

        column.OrdinalPosition = insertIndex;
        _columns.Insert(insertIndex, column);
        _columnsByName[column.Name] = column;

        // Update ordinal positions for subsequent columns
        for (int i = insertIndex + 1; i < _columns.Count; i++)
        {
            _columns[i].OrdinalPosition = i;
        }

        ModifiedAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Drops a column from the schema.
    /// For online schema change, this marks existing row data for that column as invalid.
    /// </summary>
    /// <param name="columnName">The name of the column to drop.</param>
    public void DropColumn(string columnName)
    {
        if (!_columnsByName.TryGetValue(columnName, out var column))
            throw new ArgumentException($"Column '{columnName}' not found", nameof(columnName));

        if (column.IsPrimaryKey)
            throw new InvalidOperationException($"Cannot drop primary key column '{columnName}'");

        if (_columns.Count <= 1)
            throw new InvalidOperationException("Cannot drop the last column from a table");

        var ordinal = column.OrdinalPosition;
        _columns.RemoveAt(ordinal);
        _columnsByName.Remove(columnName);

        // Update ordinal positions for subsequent columns
        for (int i = ordinal; i < _columns.Count; i++)
        {
            _columns[i].OrdinalPosition = i;
        }

        ModifiedAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Renames a column.
    /// </summary>
    /// <param name="oldName">The current column name.</param>
    /// <param name="newName">The new column name.</param>
    public void RenameColumn(string oldName, string newName)
    {
        if (!_columnsByName.TryGetValue(oldName, out var column))
            throw new ArgumentException($"Column '{oldName}' not found", nameof(oldName));

        if (_columnsByName.ContainsKey(newName))
            throw new ArgumentException($"Column '{newName}' already exists", nameof(newName));

        _columnsByName.Remove(oldName);
        column.Name = newName;
        _columnsByName[newName] = column;

        ModifiedAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Modifies a column's definition.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <param name="newDefinition">The new column definition.</param>
    public void ModifyColumn(string columnName, ColumnDefinition newDefinition)
    {
        if (!_columnsByName.TryGetValue(columnName, out var oldColumn))
            throw new ArgumentException($"Column '{columnName}' not found", nameof(columnName));

        var ordinal = oldColumn.OrdinalPosition;
        newDefinition.OrdinalPosition = ordinal;

        _columns[ordinal] = newDefinition;
        _columnsByName.Remove(columnName);
        _columnsByName[newDefinition.Name] = newDefinition;

        ModifiedAt = DateTime.UtcNow;
    }

    #endregion

    /// <summary>
    /// Calculates the maximum possible row size in bytes.
    /// </summary>
    public int CalculateMaxRowSize()
    {
        int size = 0;

        // Null bitmap: 1 bit per column, rounded up to bytes
        size += (Columns.Count + 7) / 8;

        foreach (var column in Columns)
        {
            size += column.GetByteSize();
        }

        return size;
    }

    /// <summary>
    /// Validates that a row of values matches this schema.
    /// </summary>
    public void ValidateRow(DataValue[] values)
    {
        if (values.Length != Columns.Count)
        {
            throw new ArgumentException(
                $"Row has {values.Length} values but table has {Columns.Count} columns");
        }

        for (int i = 0; i < values.Length; i++)
        {
            var column = Columns[i];
            var value = values[i];

            if (!column.ValidateValue(value))
            {
                throw new ArgumentException(
                    $"Invalid value for column '{column.Name}': {value}");
            }
        }
    }

    /// <summary>
    /// Serializes this table schema to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(TableId);
        writer.Write(DatabaseName);
        writer.Write(TableName);
        writer.Write(CreatedAt.Ticks);
        writer.Write(ModifiedAt.Ticks);
        writer.Write(AutoIncrementValue);
        writer.Write(RowCount);

        // Write columns
        writer.Write(Columns.Count);
        foreach (var column in Columns)
        {
            var columnBytes = column.Serialize();
            writer.Write(columnBytes.Length);
            writer.Write(columnBytes);
        }

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a table schema from bytes.
    /// </summary>
    public static TableSchema Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var tableId = reader.ReadInt32();
        var databaseName = reader.ReadString();
        var tableName = reader.ReadString();
        var createdAt = new DateTime(reader.ReadInt64());
        var modifiedAt = new DateTime(reader.ReadInt64());
        var autoIncrementValue = reader.ReadInt64();
        var rowCount = reader.ReadInt64();

        var columnCount = reader.ReadInt32();
        var columns = new List<ColumnDefinition>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            var columnLength = reader.ReadInt32();
            var columnBytes = reader.ReadBytes(columnLength);
            columns.Add(ColumnDefinition.Deserialize(columnBytes));
        }

        var schema = new TableSchema(tableId, databaseName, tableName, columns, createdAt)
        {
            ModifiedAt = modifiedAt,
            AutoIncrementValue = autoIncrementValue,
            RowCount = rowCount
        };

        return schema;
    }

    public override string ToString()
    {
        return $"Table {FullName} ({Columns.Count} columns)";
    }

    /// <summary>
    /// Generates a CREATE TABLE statement for this schema.
    /// </summary>
    public string ToCreateTableStatement()
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"CREATE TABLE `{TableName}` (");

        var columnDefs = Columns.Select(c => $"  {c}");
        sb.AppendLine(string.Join(",\n", columnDefs));

        if (PrimaryKeyColumns.Count > 0)
        {
            var pkCols = string.Join(", ", PrimaryKeyColumns.Select(c => $"`{c.Name}`"));
            sb.AppendLine($"  PRIMARY KEY ({pkCols})");
        }

        sb.Append(");");
        return sb.ToString();
    }
}
