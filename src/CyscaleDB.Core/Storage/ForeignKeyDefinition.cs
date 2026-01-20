using CyscaleDB.Core.Parsing.Ast;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents a foreign key constraint definition stored in the catalog.
/// This persists all foreign key metadata including referential actions.
/// </summary>
public sealed class ForeignKeyDefinition
{
    /// <summary>
    /// The name of this foreign key constraint.
    /// </summary>
    public string ConstraintName { get; }

    /// <summary>
    /// The table containing the foreign key columns.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The columns in this table that make up the foreign key.
    /// </summary>
    public IReadOnlyList<string> Columns { get; }

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
    public ForeignKeyReferentialAction OnDelete { get; }

    /// <summary>
    /// Action to take when referenced row is updated.
    /// </summary>
    public ForeignKeyReferentialAction OnUpdate { get; }

    /// <summary>
    /// When this foreign key was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Creates a new foreign key definition.
    /// </summary>
    public ForeignKeyDefinition(
        string constraintName,
        string tableName,
        IEnumerable<string> columns,
        string referencedTableName,
        IEnumerable<string> referencedColumns,
        ForeignKeyReferentialAction onDelete = ForeignKeyReferentialAction.Restrict,
        ForeignKeyReferentialAction onUpdate = ForeignKeyReferentialAction.Restrict,
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

        ConstraintName = constraintName;
        TableName = tableName;
        Columns = columnList.AsReadOnly();
        ReferencedTableName = referencedTableName;
        ReferencedColumns = refColumnList.AsReadOnly();
        OnDelete = onDelete;
        OnUpdate = onUpdate;
        CreatedAt = createdAt ?? DateTime.UtcNow;
    }

    /// <summary>
    /// Serializes this foreign key definition to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(ConstraintName);
        writer.Write(TableName);

        writer.Write(Columns.Count);
        foreach (var col in Columns)
        {
            writer.Write(col);
        }

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
    /// Deserializes a foreign key definition from bytes.
    /// </summary>
    public static ForeignKeyDefinition Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var constraintName = reader.ReadString();
        var tableName = reader.ReadString();

        var columnCount = reader.ReadInt32();
        var columns = new List<string>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            columns.Add(reader.ReadString());
        }

        var referencedTableName = reader.ReadString();

        var refColumnCount = reader.ReadInt32();
        var referencedColumns = new List<string>(refColumnCount);
        for (int i = 0; i < refColumnCount; i++)
        {
            referencedColumns.Add(reader.ReadString());
        }

        var onDelete = (ForeignKeyReferentialAction)reader.ReadByte();
        var onUpdate = (ForeignKeyReferentialAction)reader.ReadByte();
        var createdAt = new DateTime(reader.ReadInt64());

        return new ForeignKeyDefinition(
            constraintName, tableName, columns,
            referencedTableName, referencedColumns,
            onDelete, onUpdate, createdAt);
    }

    /// <summary>
    /// Creates a ForeignKeyDefinition from a TableConstraint AST node.
    /// </summary>
    public static ForeignKeyDefinition FromConstraint(string tableName, TableConstraint constraint)
    {
        if (constraint.Type != ConstraintType.ForeignKey)
            throw new ArgumentException("Constraint must be a foreign key", nameof(constraint));

        var constraintName = constraint.Name 
            ?? $"fk_{tableName}_{string.Join("_", constraint.Columns)}";

        return new ForeignKeyDefinition(
            constraintName,
            tableName,
            constraint.Columns,
            constraint.ReferencedTable!,
            constraint.ReferencedColumns,
            constraint.OnDelete,
            constraint.OnUpdate);
    }

    /// <summary>
    /// Creates a ForeignKeyDefinition from an AddForeignKeyAction AST node.
    /// </summary>
    public static ForeignKeyDefinition FromAction(string tableName, AddForeignKeyAction action)
    {
        var constraintName = action.ConstraintName 
            ?? $"fk_{tableName}_{string.Join("_", action.Columns)}";

        return new ForeignKeyDefinition(
            constraintName,
            tableName,
            action.Columns,
            action.ReferencedTable,
            action.ReferencedColumns,
            action.OnDelete,
            action.OnUpdate);
    }

    public override string ToString()
    {
        var onDeleteStr = OnDelete != ForeignKeyReferentialAction.Restrict ? $" ON DELETE {OnDelete}" : "";
        var onUpdateStr = OnUpdate != ForeignKeyReferentialAction.Restrict ? $" ON UPDATE {OnUpdate}" : "";
        return $"FK '{ConstraintName}' on {TableName}({string.Join(", ", Columns)}) " +
               $"-> {ReferencedTableName}({string.Join(", ", ReferencedColumns)}){onDeleteStr}{onUpdateStr}";
    }
}
