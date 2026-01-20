using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Parsing.Ast;

/// <summary>
/// Base class for SQL expressions.
/// </summary>
public abstract class Expression : AstNode
{
}

/// <summary>
/// Binary expression (e.g., a + b, x = y, a AND b).
/// </summary>
public class BinaryExpression : Expression
{
    /// <summary>
    /// The left operand.
    /// </summary>
    public Expression Left { get; set; } = null!;

    /// <summary>
    /// The operator.
    /// </summary>
    public BinaryOperator Operator { get; set; }

    /// <summary>
    /// The right operand.
    /// </summary>
    public Expression Right { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitBinaryExpression(this);
}

/// <summary>
/// Binary operators.
/// </summary>
public enum BinaryOperator
{
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,

    // Logical
    And,
    Or
}

/// <summary>
/// Unary expression (e.g., -x, NOT y).
/// </summary>
public class UnaryExpression : Expression
{
    /// <summary>
    /// The operator.
    /// </summary>
    public UnaryOperator Operator { get; set; }

    /// <summary>
    /// The operand.
    /// </summary>
    public Expression Operand { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitUnaryExpression(this);
}

/// <summary>
/// Unary operators.
/// </summary>
public enum UnaryOperator
{
    Negate,
    Not
}

/// <summary>
/// Literal value expression (numbers, strings, booleans, NULL).
/// </summary>
public class LiteralExpression : Expression
{
    /// <summary>
    /// The literal value.
    /// </summary>
    public DataValue Value { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitLiteralExpression(this);
}

/// <summary>
/// Column reference expression (e.g., column_name or table.column_name).
/// </summary>
public class ColumnReference : Expression
{
    /// <summary>
    /// The table name or alias (optional).
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// The column name.
    /// </summary>
    public string ColumnName { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitColumnReference(this);
}

/// <summary>
/// Function call expression (e.g., COUNT(*), UPPER(name)).
/// </summary>
public class FunctionCall : Expression
{
    /// <summary>
    /// The function name.
    /// </summary>
    public string FunctionName { get; set; } = null!;

    /// <summary>
    /// The function arguments.
    /// </summary>
    public List<Expression> Arguments { get; set; } = [];

    /// <summary>
    /// Whether DISTINCT is applied (e.g., COUNT(DISTINCT column)).
    /// </summary>
    public bool IsDistinct { get; set; }

    /// <summary>
    /// Whether this is an aggregate function with * (e.g., COUNT(*)).
    /// </summary>
    public bool IsStarArgument { get; set; }

    /// <summary>
    /// ORDER BY clause inside function (MySQL extension, e.g., COUNT(column ORDER BY column)).
    /// </summary>
    public List<Expression>? OrderBy { get; set; }

    /// <summary>
    /// SEPARATOR string for GROUP_CONCAT function (e.g., GROUP_CONCAT(column SEPARATOR ',')).
    /// </summary>
    public string? Separator { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitFunctionCall(this);
}

/// <summary>
/// Subquery expression (used in WHERE clause).
/// </summary>
public class Subquery : Expression
{
    /// <summary>
    /// The SELECT statement.
    /// </summary>
    public SelectStatement Query { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitSubquery(this);
}

/// <summary>
/// IN expression (e.g., column IN (1, 2, 3) or column IN (SELECT ...)).
/// </summary>
public class InExpression : Expression
{
    /// <summary>
    /// The expression to test.
    /// </summary>
    public Expression Expression { get; set; } = null!;

    /// <summary>
    /// The list of values (mutually exclusive with Subquery).
    /// </summary>
    public List<Expression>? Values { get; set; }

    /// <summary>
    /// The subquery (mutually exclusive with Values).
    /// </summary>
    public SelectStatement? Subquery { get; set; }

    /// <summary>
    /// Whether this is NOT IN.
    /// </summary>
    public bool IsNot { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitInExpression(this);
}

/// <summary>
/// BETWEEN expression (e.g., column BETWEEN 1 AND 10).
/// </summary>
public class BetweenExpression : Expression
{
    /// <summary>
    /// The expression to test.
    /// </summary>
    public Expression Expression { get; set; } = null!;

    /// <summary>
    /// The lower bound.
    /// </summary>
    public Expression Low { get; set; } = null!;

    /// <summary>
    /// The upper bound.
    /// </summary>
    public Expression High { get; set; } = null!;

    /// <summary>
    /// Whether this is NOT BETWEEN.
    /// </summary>
    public bool IsNot { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitBetweenExpression(this);
}

/// <summary>
/// IS NULL expression (e.g., column IS NULL, column IS NOT NULL).
/// </summary>
public class IsNullExpression : Expression
{
    /// <summary>
    /// The expression to test.
    /// </summary>
    public Expression Expression { get; set; } = null!;

    /// <summary>
    /// Whether this is IS NOT NULL.
    /// </summary>
    public bool IsNot { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitIsNullExpression(this);
}

/// <summary>
/// EXISTS expression (e.g., EXISTS (SELECT ...)).
/// </summary>
public class ExistsExpression : Expression
{
    /// <summary>
    /// The subquery.
    /// </summary>
    public SelectStatement Subquery { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitExistsExpression(this);
}

/// <summary>
/// LIKE expression (e.g., name LIKE 'John%').
/// </summary>
public class LikeExpression : Expression
{
    /// <summary>
    /// The expression being tested.
    /// </summary>
    public Expression Expression { get; set; } = null!;

    /// <summary>
    /// The pattern to match.
    /// </summary>
    public Expression Pattern { get; set; } = null!;

    /// <summary>
    /// Whether this is NOT LIKE.
    /// </summary>
    public bool IsNot { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitLikeExpression(this);
}

/// <summary>
/// System variable expression (e.g., @@version, @@sql_mode).
/// </summary>
public class SystemVariableExpression : Expression
{
    /// <summary>
    /// The variable name (without @@ prefix).
    /// </summary>
    public string VariableName { get; set; } = null!;

    /// <summary>
    /// The scope (GLOBAL or SESSION). Defaults to SESSION.
    /// </summary>
    public SetScope Scope { get; set; } = SetScope.Session;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitSystemVariableExpression(this);
}
