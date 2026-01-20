using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Parsing.Ast;

/// <summary>
/// Represents a column definition in a CREATE TABLE statement.
/// </summary>
public sealed class ColumnDefinitionNode : AstNode
{
    /// <summary>
    /// The name of the column.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// The data type of the column.
    /// </summary>
    public DataType DataType { get; }

    /// <summary>
    /// The maximum length for variable-length types.
    /// </summary>
    public int? MaxLength { get; }

    /// <summary>
    /// The precision for DECIMAL type.
    /// </summary>
    public int? Precision { get; }

    /// <summary>
    /// The scale for DECIMAL type.
    /// </summary>
    public int? Scale { get; }

    /// <summary>
    /// Whether the column allows NULL values.
    /// </summary>
    public bool IsNullable { get; }

    /// <summary>
    /// Whether this column is a primary key.
    /// </summary>
    public bool IsPrimaryKey { get; }

    /// <summary>
    /// Whether this column has auto-increment enabled.
    /// </summary>
    public bool IsAutoIncrement { get; }

    /// <summary>
    /// Whether this column has a UNIQUE constraint.
    /// </summary>
    public bool IsUnique { get; }

    /// <summary>
    /// The default value expression, or null if none.
    /// </summary>
    public Expression? DefaultValue { get; }

    public ColumnDefinitionNode(
        string name,
        DataType dataType,
        int? maxLength = null,
        int? precision = null,
        int? scale = null,
        bool isNullable = true,
        bool isPrimaryKey = false,
        bool isAutoIncrement = false,
        bool isUnique = false,
        Expression? defaultValue = null)
    {
        Name = name;
        DataType = dataType;
        MaxLength = maxLength;
        Precision = precision;
        Scale = scale;
        IsNullable = isNullable && !isPrimaryKey; // Primary keys are implicitly NOT NULL
        IsPrimaryKey = isPrimaryKey;
        IsAutoIncrement = isAutoIncrement;
        IsUnique = isUnique || isPrimaryKey; // Primary keys are implicitly UNIQUE
        DefaultValue = defaultValue;
    }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitColumnDefinitionNode(this);

    public override string ToString()
    {
        var parts = new List<string> { Name };

        // Data type with length/precision
        var typeStr = DataType.ToString().ToUpperInvariant();
        if (MaxLength.HasValue)
        {
            typeStr = $"{typeStr}({MaxLength})";
        }
        else if (Precision.HasValue)
        {
            typeStr = Scale.HasValue
                ? $"{typeStr}({Precision},{Scale})"
                : $"{typeStr}({Precision})";
        }
        parts.Add(typeStr);

        // Constraints
        if (!IsNullable) parts.Add("NOT NULL");
        if (IsPrimaryKey) parts.Add("PRIMARY KEY");
        if (IsAutoIncrement) parts.Add("AUTO_INCREMENT");
        if (IsUnique && !IsPrimaryKey) parts.Add("UNIQUE");
        if (DefaultValue != null) parts.Add($"DEFAULT {DefaultValue}");

        return string.Join(" ", parts);
    }
}
