using CyscaleDB.Core.Parsing.Ast;

namespace CyscaleDB.Core.Storage;

/// <summary>
/// Represents a CHECK constraint definition stored in the catalog.
/// </summary>
public sealed class CheckConstraintDefinition
{
    /// <summary>
    /// The name of this constraint.
    /// </summary>
    public string ConstraintName { get; }

    /// <summary>
    /// The table this constraint belongs to.
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// The check expression as a string (SQL expression).
    /// This is stored for persistence and display purposes.
    /// </summary>
    public string ExpressionText { get; }

    /// <summary>
    /// The parsed check expression (transient, not serialized).
    /// </summary>
    public Expression? Expression { get; set; }

    /// <summary>
    /// When this constraint was created.
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// Creates a new check constraint definition.
    /// </summary>
    public CheckConstraintDefinition(
        string constraintName,
        string tableName,
        string expressionText,
        Expression? expression = null,
        DateTime? createdAt = null)
    {
        if (string.IsNullOrWhiteSpace(constraintName))
            throw new ArgumentException("Constraint name cannot be empty", nameof(constraintName));

        ConstraintName = constraintName;
        TableName = tableName;
        ExpressionText = expressionText;
        Expression = expression;
        CreatedAt = createdAt ?? DateTime.UtcNow;
    }

    /// <summary>
    /// Serializes this check constraint definition to bytes.
    /// </summary>
    public byte[] Serialize()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        writer.Write(ConstraintName);
        writer.Write(TableName);
        writer.Write(ExpressionText);
        writer.Write(CreatedAt.Ticks);

        return stream.ToArray();
    }

    /// <summary>
    /// Deserializes a check constraint definition from bytes.
    /// </summary>
    public static CheckConstraintDefinition Deserialize(byte[] data)
    {
        using var stream = new MemoryStream(data);
        using var reader = new BinaryReader(stream);

        var constraintName = reader.ReadString();
        var tableName = reader.ReadString();
        var expressionText = reader.ReadString();
        var createdAt = new DateTime(reader.ReadInt64());

        return new CheckConstraintDefinition(
            constraintName, tableName, expressionText,
            expression: null, createdAt: createdAt);
    }

    /// <summary>
    /// Creates a CheckConstraintDefinition from a TableConstraint AST node.
    /// </summary>
    public static CheckConstraintDefinition FromConstraint(string tableName, TableConstraint constraint)
    {
        if (constraint.Type != ConstraintType.Check || constraint.CheckExpression == null)
            throw new ArgumentException("Constraint must be a CHECK constraint with an expression", nameof(constraint));

        var constraintName = constraint.Name 
            ?? $"chk_{tableName}_{Guid.NewGuid():N}"[..32];

        return new CheckConstraintDefinition(
            constraintName,
            tableName,
            constraint.CheckExpression.ToString() ?? "",
            constraint.CheckExpression);
    }

    public override string ToString()
    {
        return $"CHECK '{ConstraintName}' on {TableName}: {ExpressionText}";
    }
}
