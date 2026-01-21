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
/// Quantified comparison expression (e.g., x > ALL (SELECT ...), y = ANY (SELECT ...)).
/// Supports ALL, ANY, and SOME quantifiers.
/// </summary>
public class QuantifiedComparisonExpression : Expression
{
    /// <summary>
    /// The left expression to compare.
    /// </summary>
    public Expression Expression { get; set; } = null!;

    /// <summary>
    /// The comparison operator (=, !=, <, <=, >, >=).
    /// </summary>
    public BinaryOperator Operator { get; set; }

    /// <summary>
    /// The quantifier (ALL, ANY, SOME).
    /// </summary>
    public QuantifierType Quantifier { get; set; }

    /// <summary>
    /// The subquery.
    /// </summary>
    public SelectStatement Subquery { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitQuantifiedComparisonExpression(this);
}

/// <summary>
/// Quantifier types for subquery comparisons.
/// </summary>
public enum QuantifierType
{
    /// <summary>
    /// ALL: condition must be true for all rows in subquery.
    /// </summary>
    All,

    /// <summary>
    /// ANY: condition must be true for at least one row in subquery.
    /// </summary>
    Any,

    /// <summary>
    /// SOME: synonym for ANY.
    /// </summary>
    Some
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

/// <summary>
/// A single WHEN...THEN branch in a CASE expression.
/// </summary>
public class WhenClause
{
    /// <summary>
    /// The condition expression (for searched CASE) or the value to compare (for simple CASE).
    /// </summary>
    public Expression When { get; set; } = null!;

    /// <summary>
    /// The result expression when the condition is true.
    /// </summary>
    public Expression Then { get; set; } = null!;
}

/// <summary>
/// CASE expression.
/// Supports both searched CASE (CASE WHEN cond THEN result...)
/// and simple CASE (CASE expr WHEN value THEN result...).
/// </summary>
public class CaseExpression : Expression
{
    /// <summary>
    /// The operand expression for simple CASE (CASE operand WHEN value...).
    /// Null for searched CASE (CASE WHEN condition...).
    /// </summary>
    public Expression? Operand { get; set; }

    /// <summary>
    /// The WHEN...THEN branches.
    /// </summary>
    public List<WhenClause> WhenClauses { get; set; } = [];

    /// <summary>
    /// The ELSE expression, or null if no ELSE clause.
    /// </summary>
    public Expression? ElseResult { get; set; }

    /// <summary>
    /// Whether this is a simple CASE (with an operand) or a searched CASE.
    /// </summary>
    public bool IsSimpleCase => Operand != null;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCaseExpression(this);
}

/// <summary>
/// Window function call expression (e.g., ROW_NUMBER() OVER (PARTITION BY col ORDER BY col)).
/// </summary>
public class WindowFunctionCall : Expression
{
    /// <summary>
    /// The function name (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.).
    /// </summary>
    public string FunctionName { get; set; } = null!;

    /// <summary>
    /// The function arguments (for LAG, LEAD, FIRST_VALUE, etc.).
    /// </summary>
    public List<Expression> Arguments { get; set; } = [];

    /// <summary>
    /// The window specification.
    /// </summary>
    public WindowSpec WindowSpec { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitWindowFunctionCall(this);
}

/// <summary>
/// Window specification (OVER clause).
/// </summary>
public class WindowSpec
{
    /// <summary>
    /// Optional name of a pre-defined window.
    /// </summary>
    public string? WindowName { get; set; }

    /// <summary>
    /// PARTITION BY expressions.
    /// </summary>
    public List<Expression> PartitionBy { get; set; } = [];

    /// <summary>
    /// ORDER BY clauses within the window.
    /// </summary>
    public List<OrderByClause> OrderBy { get; set; } = [];

    /// <summary>
    /// Frame clause specification (ROWS or RANGE).
    /// </summary>
    public WindowFrame? Frame { get; set; }
}

/// <summary>
/// Window frame specification (ROWS/RANGE BETWEEN ... AND ...).
/// </summary>
public class WindowFrame
{
    /// <summary>
    /// Frame type (ROWS or RANGE).
    /// </summary>
    public WindowFrameType FrameType { get; set; }

    /// <summary>
    /// The start of the frame.
    /// </summary>
    public WindowFrameBound Start { get; set; } = null!;

    /// <summary>
    /// The end of the frame (optional, defaults to CURRENT ROW if not specified).
    /// </summary>
    public WindowFrameBound? End { get; set; }
}

/// <summary>
/// Window frame type.
/// </summary>
public enum WindowFrameType
{
    Rows,
    Range
}

/// <summary>
/// Window frame bound (UNBOUNDED PRECEDING, CURRENT ROW, N PRECEDING, etc.).
/// </summary>
public class WindowFrameBound
{
    /// <summary>
    /// The type of bound.
    /// </summary>
    public WindowFrameBoundType BoundType { get; set; }

    /// <summary>
    /// The offset value (for N PRECEDING/FOLLOWING).
    /// </summary>
    public int? Offset { get; set; }
}

/// <summary>
/// Window frame bound types.
/// </summary>
public enum WindowFrameBoundType
{
    /// <summary>
    /// UNBOUNDED PRECEDING
    /// </summary>
    UnboundedPreceding,

    /// <summary>
    /// UNBOUNDED FOLLOWING
    /// </summary>
    UnboundedFollowing,

    /// <summary>
    /// CURRENT ROW
    /// </summary>
    CurrentRow,

    /// <summary>
    /// N PRECEDING
    /// </summary>
    Preceding,

    /// <summary>
    /// N FOLLOWING
    /// </summary>
    Following
}

/// <summary>
/// MATCH...AGAINST full-text search expression.
/// Example: MATCH(col1, col2) AGAINST('search text' IN NATURAL LANGUAGE MODE)
/// </summary>
public class MatchExpression : Expression
{
    /// <summary>
    /// The columns to search in.
    /// </summary>
    public List<ColumnReference> Columns { get; set; } = new();

    /// <summary>
    /// The search text.
    /// </summary>
    public Expression SearchText { get; set; } = null!;

    /// <summary>
    /// The search mode.
    /// </summary>
    public MatchSearchMode Mode { get; set; } = MatchSearchMode.NaturalLanguage;

    /// <summary>
    /// Whether to use query expansion.
    /// </summary>
    public bool WithQueryExpansion { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitMatchExpression(this);
}

/// <summary>
/// Mode for MATCH...AGAINST full-text search.
/// </summary>
public enum MatchSearchMode
{
    /// <summary>
    /// Natural language mode (default).
    /// </summary>
    NaturalLanguage,

    /// <summary>
    /// Boolean mode with operators (+, -, *, etc.).
    /// </summary>
    Boolean,

    /// <summary>
    /// Natural language with query expansion.
    /// </summary>
    NaturalLanguageWithQueryExpansion
}
