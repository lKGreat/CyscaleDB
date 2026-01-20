using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Parsing.Ast;

/// <summary>
/// Base class for SQL statements.
/// </summary>
public abstract class Statement : AstNode
{
}

#region SELECT Statement

/// <summary>
/// Represents a SELECT statement.
/// </summary>
public class SelectStatement : Statement
{
    /// <summary>
    /// Whether DISTINCT is specified.
    /// </summary>
    public bool IsDistinct { get; set; }

    /// <summary>
    /// The columns to select.
    /// </summary>
    public List<SelectColumn> Columns { get; set; } = [];

    /// <summary>
    /// The FROM clause (table references).
    /// </summary>
    public TableReference? From { get; set; }

    /// <summary>
    /// The WHERE clause condition.
    /// </summary>
    public Expression? Where { get; set; }

    /// <summary>
    /// The GROUP BY columns.
    /// </summary>
    public List<Expression> GroupBy { get; set; } = [];

    /// <summary>
    /// The HAVING clause condition.
    /// </summary>
    public Expression? Having { get; set; }

    /// <summary>
    /// The ORDER BY clauses.
    /// </summary>
    public List<OrderByClause> OrderBy { get; set; } = [];

    /// <summary>
    /// The LIMIT value.
    /// </summary>
    public int? Limit { get; set; }

    /// <summary>
    /// The OFFSET value.
    /// </summary>
    public int? Offset { get; set; }

    /// <summary>
    /// UNION queries (for SELECT ... UNION SELECT ...)
    /// </summary>
    public List<SelectStatement> UnionQueries { get; set; } = [];

    /// <summary>
    /// Whether UNION ALL is used (vs UNION which removes duplicates).
    /// </summary>
    public List<bool> UnionAllFlags { get; set; } = [];

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitSelectStatement(this);
}

/// <summary>
/// Represents a column in a SELECT list.
/// </summary>
public class SelectColumn
{
    /// <summary>
    /// The expression for this column (can be *, column reference, or complex expression).
    /// </summary>
    public Expression Expression { get; set; } = null!;

    /// <summary>
    /// The alias for this column (AS clause).
    /// </summary>
    public string? Alias { get; set; }

    /// <summary>
    /// Whether this is a SELECT * wildcard.
    /// </summary>
    public bool IsWildcard { get; set; }

    /// <summary>
    /// For table.* syntax, the table qualifier.
    /// </summary>
    public string? TableQualifier { get; set; }
}

/// <summary>
/// Represents an item in a SELECT list (alias for SelectColumn for compatibility).
/// </summary>
public class SelectItem
{
    /// <summary>
    /// The expression for this item.
    /// </summary>
    public Expression? Expression { get; set; }

    /// <summary>
    /// The alias for this item.
    /// </summary>
    public string? Alias { get; set; }

    /// <summary>
    /// Whether this is a wildcard (*).
    /// </summary>
    public bool IsWildcard { get; set; }

    /// <summary>
    /// Table name for qualified wildcard (table.*).
    /// </summary>
    public string? TableName { get; set; }

    public SelectItem() { }

    public SelectItem(Expression expression, string? alias = null)
    {
        Expression = expression;
        Alias = alias;
    }

    public static SelectItem Wildcard() => new() { IsWildcard = true };
    
    public static SelectItem QualifiedWildcard(string tableName) => 
        new() { IsWildcard = true, TableName = tableName };
}

/// <summary>
/// Represents a JOIN clause.
/// </summary>
public class JoinClause
{
    /// <summary>
    /// The type of join.
    /// </summary>
    public JoinType JoinType { get; set; }

    /// <summary>
    /// The table to join.
    /// </summary>
    public SimpleTableReference Table { get; set; } = null!;

    /// <summary>
    /// The join condition (ON clause).
    /// </summary>
    public Expression? Condition { get; set; }

    public JoinClause() { }

    public JoinClause(JoinType joinType, SimpleTableReference table, Expression? condition)
    {
        JoinType = joinType;
        Table = table;
        Condition = condition;
    }
}

/// <summary>
/// Represents an ORDER BY clause.
/// </summary>
public class OrderByClause
{
    /// <summary>
    /// The expression to order by.
    /// </summary>
    public Expression Expression { get; set; } = null!;

    /// <summary>
    /// Whether to sort descending.
    /// </summary>
    public bool Descending { get; set; }
}

#endregion

#region Table References (FROM clause)

/// <summary>
/// Base class for table references in FROM clause.
/// </summary>
public abstract class TableReference
{
}

/// <summary>
/// A simple table reference (table name).
/// </summary>
public class SimpleTableReference : TableReference
{
    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The alias for this table.
    /// </summary>
    public string? Alias { get; set; }
}

/// <summary>
/// A JOIN table reference.
/// </summary>
public class JoinTableReference : TableReference
{
    /// <summary>
    /// The left side of the join.
    /// </summary>
    public TableReference Left { get; set; } = null!;

    /// <summary>
    /// The right side of the join.
    /// </summary>
    public TableReference Right { get; set; } = null!;

    /// <summary>
    /// The type of join.
    /// </summary>
    public JoinType JoinType { get; set; }

    /// <summary>
    /// The join condition (ON clause).
    /// </summary>
    public Expression? Condition { get; set; }
}

/// <summary>
/// Types of JOINs.
/// </summary>
public enum JoinType
{
    Inner,
    Left,
    Right,
    Full,
    Cross
}

/// <summary>
/// A subquery used as a table reference.
/// </summary>
public class SubqueryTableReference : TableReference
{
    /// <summary>
    /// The subquery.
    /// </summary>
    public SelectStatement Subquery { get; set; } = null!;

    /// <summary>
    /// The alias for this subquery (required).
    /// </summary>
    public string Alias { get; set; } = null!;
}

#endregion

#region INSERT Statement

/// <summary>
/// Represents an INSERT statement.
/// </summary>
public class InsertStatement : Statement
{
    /// <summary>
    /// The target table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// The columns to insert into (optional - if empty, all columns in order).
    /// </summary>
    public List<string> Columns { get; set; } = [];

    /// <summary>
    /// The values to insert.
    /// </summary>
    public List<List<Expression>> ValuesList { get; set; } = [];

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitInsertStatement(this);
}

#endregion

#region UPDATE Statement

/// <summary>
/// Represents an UPDATE statement.
/// </summary>
public class UpdateStatement : Statement
{
    /// <summary>
    /// The target table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// The SET assignments.
    /// </summary>
    public List<SetClause> SetClauses { get; set; } = [];

    /// <summary>
    /// The WHERE condition.
    /// </summary>
    public Expression? Where { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitUpdateStatement(this);
}

/// <summary>
/// Represents a SET clause in UPDATE.
/// </summary>
public class SetClause
{
    /// <summary>
    /// The column name.
    /// </summary>
    public string ColumnName { get; set; } = null!;

    /// <summary>
    /// The value expression.
    /// </summary>
    public Expression Value { get; set; } = null!;
}

#endregion

#region DELETE Statement

/// <summary>
/// Represents a DELETE statement.
/// </summary>
public class DeleteStatement : Statement
{
    /// <summary>
    /// The target table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// The WHERE condition.
    /// </summary>
    public Expression? Where { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDeleteStatement(this);
}

#endregion

#region DDL Statements

/// <summary>
/// Represents a CREATE TABLE statement.
/// </summary>
public class CreateTableStatement : Statement
{
    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Whether IF NOT EXISTS is specified.
    /// </summary>
    public bool IfNotExists { get; set; }

    /// <summary>
    /// The column definitions.
    /// </summary>
    public List<ColumnDef> Columns { get; set; } = [];

    /// <summary>
    /// Table-level constraints.
    /// </summary>
    public List<TableConstraint> Constraints { get; set; } = [];

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateTableStatement(this);
}

/// <summary>
/// Represents a column definition in CREATE TABLE.
/// </summary>
public class ColumnDef
{
    /// <summary>
    /// The column name.
    /// </summary>
    public string Name { get; set; } = null!;

    /// <summary>
    /// The data type.
    /// </summary>
    public DataType DataType { get; set; }

    /// <summary>
    /// Maximum length for VARCHAR/CHAR.
    /// </summary>
    public int? Length { get; set; }

    /// <summary>
    /// Precision for DECIMAL.
    /// </summary>
    public int? Precision { get; set; }

    /// <summary>
    /// Scale for DECIMAL.
    /// </summary>
    public int? Scale { get; set; }

    /// <summary>
    /// Whether the column allows NULL.
    /// </summary>
    public bool IsNullable { get; set; } = true;

    /// <summary>
    /// Whether this column is a primary key.
    /// </summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// Whether AUTO_INCREMENT is enabled.
    /// </summary>
    public bool IsAutoIncrement { get; set; }

    /// <summary>
    /// Whether the column has a UNIQUE constraint.
    /// </summary>
    public bool IsUnique { get; set; }

    /// <summary>
    /// The default value expression.
    /// </summary>
    public Expression? DefaultValue { get; set; }
}

/// <summary>
/// Represents a table-level constraint.
/// </summary>
public class TableConstraint
{
    /// <summary>
    /// The constraint name (optional).
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// The constraint type.
    /// </summary>
    public ConstraintType Type { get; set; }

    /// <summary>
    /// The columns involved in this constraint.
    /// </summary>
    public List<string> Columns { get; set; } = [];

    /// <summary>
    /// For FOREIGN KEY: the referenced table.
    /// </summary>
    public string? ReferencedTable { get; set; }

    /// <summary>
    /// For FOREIGN KEY: the referenced columns.
    /// </summary>
    public List<string> ReferencedColumns { get; set; } = [];
}

/// <summary>
/// Types of table constraints.
/// </summary>
public enum ConstraintType
{
    PrimaryKey,
    Unique,
    ForeignKey,
    Check
}

/// <summary>
/// Represents a DROP TABLE statement.
/// </summary>
public class DropTableStatement : Statement
{
    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Whether IF EXISTS is specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropTableStatement(this);
}

/// <summary>
/// Represents a CREATE DATABASE statement.
/// </summary>
public class CreateDatabaseStatement : Statement
{
    /// <summary>
    /// The database name.
    /// </summary>
    public string DatabaseName { get; set; } = null!;

    /// <summary>
    /// Whether IF NOT EXISTS is specified.
    /// </summary>
    public bool IfNotExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateDatabaseStatement(this);
}

/// <summary>
/// Represents a DROP DATABASE statement.
/// </summary>
public class DropDatabaseStatement : Statement
{
    /// <summary>
    /// The database name.
    /// </summary>
    public string DatabaseName { get; set; } = null!;

    /// <summary>
    /// Whether IF EXISTS is specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropDatabaseStatement(this);
}

#endregion

#region Utility Statements

/// <summary>
/// Represents a USE database statement.
/// </summary>
public class UseDatabaseStatement : Statement
{
    public string DatabaseName { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitUseDatabaseStatement(this);
}

/// <summary>
/// Represents a SHOW TABLES statement.
/// </summary>
public class ShowTablesStatement : Statement
{
    public string? DatabaseName { get; set; }
    
    /// <summary>
    /// Whether FULL keyword was specified (SHOW FULL TABLES).
    /// </summary>
    public bool IsFull { get; set; }
    
    /// <summary>
    /// Optional WHERE clause filter (e.g., SHOW TABLES WHERE Table_type != 'VIEW').
    /// </summary>
    public Expression? Where { get; set; }
    
    /// <summary>
    /// Optional LIKE pattern (e.g., SHOW TABLES LIKE 'user%').
    /// </summary>
    public string? LikePattern { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowTablesStatement(this);
}

/// <summary>
/// Represents a SHOW DATABASES statement.
/// </summary>
public class ShowDatabasesStatement : Statement
{
    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowDatabasesStatement(this);
}

/// <summary>
/// Represents a DESCRIBE statement.
/// </summary>
public class DescribeStatement : Statement
{
    public string TableName { get; set; } = null!;
    public string? DatabaseName { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDescribeStatement(this);
}

#endregion

#region Index Statements

/// <summary>
/// Represents a CREATE INDEX statement.
/// </summary>
public class CreateIndexStatement : Statement
{
    /// <summary>
    /// The name of the index.
    /// </summary>
    public string IndexName { get; set; } = null!;

    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// The columns to include in the index.
    /// </summary>
    public List<string> Columns { get; set; } = [];

    /// <summary>
    /// The index type (BTREE or HASH).
    /// </summary>
    public IndexTypeAst IndexType { get; set; } = IndexTypeAst.BTree;

    /// <summary>
    /// Whether this is a unique index.
    /// </summary>
    public bool IsUnique { get; set; }

    /// <summary>
    /// Whether IF NOT EXISTS is specified.
    /// </summary>
    public bool IfNotExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateIndexStatement(this);
}

/// <summary>
/// Represents a DROP INDEX statement.
/// </summary>
public class DropIndexStatement : Statement
{
    /// <summary>
    /// The name of the index.
    /// </summary>
    public string IndexName { get; set; } = null!;

    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Whether IF EXISTS is specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropIndexStatement(this);
}

/// <summary>
/// Index type in AST.
/// </summary>
public enum IndexTypeAst
{
    BTree,
    Hash
}

#endregion

#region View Statements

/// <summary>
/// Represents a CREATE VIEW statement.
/// </summary>
public class CreateViewStatement : Statement
{
    /// <summary>
    /// The name of the view.
    /// </summary>
    public string ViewName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Optional column names for the view.
    /// </summary>
    public List<string>? ColumnNames { get; set; }

    /// <summary>
    /// The SELECT statement defining this view.
    /// </summary>
    public SelectStatement Query { get; set; } = null!;

    /// <summary>
    /// Whether this is CREATE OR REPLACE.
    /// </summary>
    public bool OrReplace { get; set; }

    /// <summary>
    /// Whether IF NOT EXISTS is specified.
    /// </summary>
    public bool IfNotExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateViewStatement(this);
}

/// <summary>
/// Represents a DROP VIEW statement.
/// </summary>
public class DropViewStatement : Statement
{
    /// <summary>
    /// The name of the view.
    /// </summary>
    public string ViewName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Whether IF EXISTS is specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropViewStatement(this);
}

#endregion

#region Optimization Statements

/// <summary>
/// Represents an OPTIMIZE TABLE statement.
/// </summary>
public class OptimizeTableStatement : Statement
{
    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitOptimizeTableStatement(this);
}

#endregion

#region Transaction Statements

/// <summary>
/// Represents a BEGIN transaction statement.
/// </summary>
public class BeginStatement : Statement
{
    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitBeginStatement(this);
}

/// <summary>
/// Represents a COMMIT statement.
/// </summary>
public class CommitStatement : Statement
{
    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCommitStatement(this);
}

/// <summary>
/// Represents a ROLLBACK statement.
/// </summary>
public class RollbackStatement : Statement
{
    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitRollbackStatement(this);
}

#endregion

#region SET Statements

/// <summary>
/// Scope for SET statement variables.
/// </summary>
public enum SetScope
{
    Session,
    Global
}

/// <summary>
/// Represents a variable assignment in a SET statement.
/// </summary>
public class SetVariable
{
    /// <summary>
    /// The variable name.
    /// </summary>
    public string Name { get; set; } = null!;

    /// <summary>
    /// The value expression.
    /// </summary>
    public Expression Value { get; set; } = null!;

    /// <summary>
    /// The scope for this variable (SESSION or GLOBAL).
    /// </summary>
    public SetScope Scope { get; set; } = SetScope.Session;
}

/// <summary>
/// Represents a SET statement.
/// </summary>
public class SetStatement : Statement
{
    /// <summary>
    /// The variables to set.
    /// </summary>
    public List<SetVariable> Variables { get; set; } = [];

    /// <summary>
    /// For SET NAMES charset [COLLATE collation].
    /// </summary>
    public bool IsSetNames { get; set; }

    /// <summary>
    /// The charset for SET NAMES.
    /// </summary>
    public string? Charset { get; set; }

    /// <summary>
    /// The collation for SET NAMES.
    /// </summary>
    public string? Collation { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitSetStatement(this);
}

#endregion

#region Extended SHOW Statements

/// <summary>
/// Represents a SHOW VARIABLES statement.
/// </summary>
public class ShowVariablesStatement : Statement
{
    /// <summary>
    /// Whether to show GLOBAL or SESSION variables.
    /// </summary>
    public SetScope Scope { get; set; } = SetScope.Session;

    /// <summary>
    /// LIKE pattern for filtering.
    /// </summary>
    public string? LikePattern { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowVariablesStatement(this);
}

/// <summary>
/// Represents a SHOW STATUS statement.
/// </summary>
public class ShowStatusStatement : Statement
{
    /// <summary>
    /// Whether to show GLOBAL or SESSION status.
    /// </summary>
    public SetScope Scope { get; set; } = SetScope.Session;

    /// <summary>
    /// LIKE pattern for filtering.
    /// </summary>
    public string? LikePattern { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowStatusStatement(this);
}

/// <summary>
/// Represents a SHOW CREATE TABLE statement.
/// </summary>
public class ShowCreateTableStatement : Statement
{
    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowCreateTableStatement(this);
}

/// <summary>
/// Represents a SHOW COLUMNS/SHOW FIELDS statement.
/// </summary>
public class ShowColumnsStatement : Statement
{
    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// LIKE pattern for filtering.
    /// </summary>
    public string? LikePattern { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowColumnsStatement(this);
}

/// <summary>
/// Represents a SHOW TABLE STATUS statement.
/// </summary>
public class ShowTableStatusStatement : Statement
{
    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// LIKE pattern for filtering.
    /// </summary>
    public string? LikePattern { get; set; }
    
    /// <summary>
    /// WHERE clause for filtering.
    /// </summary>
    public Expression? Where { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowTableStatusStatement(this);
}

/// <summary>
/// Represents a SHOW INDEX/SHOW INDEXES/SHOW KEYS statement.
/// </summary>
public class ShowIndexStatement : Statement
{
    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The database name (optional).
    /// </summary>
    public string? DatabaseName { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowIndexStatement(this);
}

/// <summary>
/// Represents a SHOW WARNINGS statement.
/// </summary>
public class ShowWarningsStatement : Statement
{
    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowWarningsStatement(this);
}

/// <summary>
/// Represents a SHOW ERRORS statement.
/// </summary>
public class ShowErrorsStatement : Statement
{
    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowErrorsStatement(this);
}

/// <summary>
/// Represents a SHOW COLLATION statement.
/// </summary>
public class ShowCollationStatement : Statement
{
    /// <summary>
    /// LIKE pattern for filtering.
    /// </summary>
    public string? LikePattern { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowCollationStatement(this);
}

/// <summary>
/// Represents a SHOW CHARSET/CHARACTER SET statement.
/// </summary>
public class ShowCharsetStatement : Statement
{
    /// <summary>
    /// LIKE pattern for filtering.
    /// </summary>
    public string? LikePattern { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitShowCharsetStatement(this);
}

#endregion
