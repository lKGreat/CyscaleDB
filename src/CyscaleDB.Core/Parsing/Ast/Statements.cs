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
    /// The WITH clause (CTEs) for this SELECT.
    /// </summary>
    public WithClause? WithClause { get; set; }

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
    /// Set operation queries (for SELECT ... UNION/INTERSECT/EXCEPT SELECT ...)
    /// </summary>
    public List<SelectStatement> SetOperationQueries { get; set; } = [];

    /// <summary>
    /// Types of set operations (UNION, INTERSECT, EXCEPT).
    /// </summary>
    public List<SetOperationType> SetOperationTypes { get; set; } = [];

    /// <summary>
    /// Whether ALL is used for each set operation (vs removing duplicates).
    /// </summary>
    public List<bool> SetOperationAllFlags { get; set; } = [];

    /// <summary>
    /// UNION queries (for SELECT ... UNION SELECT ...)
    /// Deprecated: Use SetOperationQueries instead.
    /// </summary>
    public List<SelectStatement> UnionQueries { get; set; } = [];

    /// <summary>
    /// Whether UNION ALL is used (vs UNION which removes duplicates).
    /// Deprecated: Use SetOperationAllFlags instead.
    /// </summary>
    public List<bool> UnionAllFlags { get; set; } = [];

    /// <summary>
    /// Locking mode for the SELECT statement (FOR UPDATE/FOR SHARE).
    /// </summary>
    public SelectLockMode LockMode { get; set; } = SelectLockMode.None;

    /// <summary>
    /// Tables to lock (for FOR UPDATE OF table1, table2).
    /// If empty, locks all tables in the query.
    /// </summary>
    public List<string> LockTables { get; set; } = [];

    /// <summary>
    /// Whether to use NOWAIT (fail immediately if lock cannot be acquired).
    /// </summary>
    public bool NoWait { get; set; }

    /// <summary>
    /// Whether to use SKIP LOCKED (skip rows that are locked).
    /// </summary>
    public bool SkipLocked { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitSelectStatement(this);
}

/// <summary>
/// Represents a WITH clause containing one or more CTEs (Common Table Expressions).
/// </summary>
public class WithClause
{
    /// <summary>
    /// Whether this is a recursive CTE (WITH RECURSIVE).
    /// </summary>
    public bool IsRecursive { get; set; }

    /// <summary>
    /// The list of CTE definitions.
    /// </summary>
    public List<CteDefinition> Ctes { get; set; } = [];
}

/// <summary>
/// Represents a single CTE (Common Table Expression) definition.
/// </summary>
public class CteDefinition
{
    /// <summary>
    /// The name of the CTE (used to reference it in the main query).
    /// </summary>
    public string Name { get; set; } = null!;

    /// <summary>
    /// Optional column names for the CTE.
    /// </summary>
    public List<string> Columns { get; set; } = [];

    /// <summary>
    /// The SELECT query that defines the CTE.
    /// </summary>
    public SelectStatement Query { get; set; } = null!;
}

/// <summary>
/// Set operation types for combining SELECT statements.
/// </summary>
public enum SetOperationType
{
    /// <summary>
    /// UNION: Combines results from two queries, removing duplicates (or keeping with ALL).
    /// </summary>
    Union = 0,

    /// <summary>
    /// INTERSECT: Returns only rows that appear in both queries.
    /// </summary>
    Intersect = 1,

    /// <summary>
    /// EXCEPT: Returns rows from first query that are not in second query.
    /// </summary>
    Except = 2
}

/// <summary>
/// Locking modes for SELECT statements.
/// </summary>
public enum SelectLockMode
{
    /// <summary>
    /// No locking (normal SELECT for snapshot read).
    /// </summary>
    None = 0,

    /// <summary>
    /// FOR UPDATE - acquires exclusive locks on selected rows.
    /// Prevents other transactions from reading (in some modes) or modifying the rows.
    /// </summary>
    ForUpdate = 1,

    /// <summary>
    /// FOR SHARE (or LOCK IN SHARE MODE) - acquires shared locks on selected rows.
    /// Allows other transactions to read but not modify the rows.
    /// </summary>
    ForShare = 2
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

    /// <summary>
    /// Whether this is a NATURAL join (implicit equality on same-named columns).
    /// </summary>
    public bool IsNatural { get; set; }

    /// <summary>
    /// The USING clause columns (explicit list of columns to join on).
    /// </summary>
    public List<string> UsingColumns { get; set; } = [];

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

    /// <summary>
    /// Whether this is a NATURAL join.
    /// </summary>
    public bool IsNatural { get; set; }

    /// <summary>
    /// The USING clause columns.
    /// </summary>
    public List<string> UsingColumns { get; set; } = [];
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
    Cross,
    Natural
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

/// <summary>
/// A reference to a CTE (Common Table Expression) in the FROM clause.
/// This is used when parsing to mark references to CTEs defined in WITH clauses.
/// </summary>
public class CteTableReference : TableReference
{
    /// <summary>
    /// The name of the CTE being referenced.
    /// </summary>
    public string CteName { get; set; } = null!;

    /// <summary>
    /// The alias for this CTE reference (optional).
    /// </summary>
    public string? Alias { get; set; }
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

    /// <summary>
    /// For ENUM type: the allowed values.
    /// </summary>
    public List<string>? EnumValues { get; set; }

    /// <summary>
    /// For SET type: the allowed values.
    /// </summary>
    public List<string>? SetValues { get; set; }

    /// <summary>
    /// Character set for string types (e.g., utf8mb4).
    /// </summary>
    public string? CharacterSet { get; set; }

    /// <summary>
    /// Collation for string types (e.g., utf8mb4_general_ci).
    /// </summary>
    public string? Collation { get; set; }

    /// <summary>
    /// Whether the numeric type is unsigned.
    /// </summary>
    public bool IsUnsigned { get; set; }

    /// <summary>
    /// Whether the numeric type has zero-fill (implies unsigned).
    /// </summary>
    public bool IsZerofill { get; set; }

    /// <summary>
    /// Column comment.
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>
    /// Whether the column has ON UPDATE CURRENT_TIMESTAMP.
    /// </summary>
    public bool OnUpdateCurrentTimestamp { get; set; }
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

    /// <summary>
    /// For FOREIGN KEY: the action to take on DELETE.
    /// </summary>
    public ForeignKeyReferentialAction OnDelete { get; set; } = ForeignKeyReferentialAction.Restrict;

    /// <summary>
    /// For FOREIGN KEY: the action to take on UPDATE.
    /// </summary>
    public ForeignKeyReferentialAction OnUpdate { get; set; } = ForeignKeyReferentialAction.Restrict;

    /// <summary>
    /// For CHECK: the check expression.
    /// </summary>
    public Expression? CheckExpression { get; set; }
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
/// Referential actions for foreign key constraints.
/// Specifies the action to take when the referenced row is deleted or updated.
/// </summary>
public enum ForeignKeyReferentialAction
{
    /// <summary>
    /// Reject the delete or update operation if there are dependent rows.
    /// This is the default behavior.
    /// </summary>
    Restrict,

    /// <summary>
    /// Same as RESTRICT but checked at the end of the statement.
    /// </summary>
    NoAction,

    /// <summary>
    /// Automatically delete or update the dependent rows.
    /// </summary>
    Cascade,

    /// <summary>
    /// Set the foreign key column(s) to NULL.
    /// </summary>
    SetNull,

    /// <summary>
    /// Set the foreign key column(s) to their default values.
    /// </summary>
    SetDefault
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

/// <summary>
/// Represents an ALTER TABLE statement.
/// </summary>
public class AlterTableStatement : Statement
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
    /// The list of alterations to perform.
    /// </summary>
    public List<AlterTableAction> Actions { get; set; } = [];

    /// <summary>
    /// The ALTER algorithm: INPLACE (online), COPY (traditional), or DEFAULT.
    /// </summary>
    public AlterAlgorithm? Algorithm { get; set; }

    /// <summary>
    /// The locking mode: NONE (no locks), SHARED (read locks), EXCLUSIVE, or DEFAULT.
    /// </summary>
    public AlterLockMode? Lock { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitAlterTableStatement(this);
}

/// <summary>
/// ALTER TABLE algorithm options.
/// </summary>
public enum AlterAlgorithm
{
    Default,
    Inplace,    // Online DDL - modify in place without copying
    Copy        // Traditional DDL - copy entire table
}

/// <summary>
/// ALTER TABLE locking modes.
/// </summary>
public enum AlterLockMode
{
    Default,
    None,       // No locks - allow concurrent reads and writes
    Shared,     // Shared locks - allow concurrent reads only
    Exclusive   // Exclusive lock - no concurrent access
}

/// <summary>
/// Base class for ALTER TABLE actions.
/// </summary>
public abstract class AlterTableAction
{
}

/// <summary>
/// ADD COLUMN action.
/// </summary>
public class AddColumnAction : AlterTableAction
{
    /// <summary>
    /// The column definition.
    /// </summary>
    public ColumnDef Column { get; set; } = null!;

    /// <summary>
    /// Optional position - FIRST or AFTER column_name.
    /// </summary>
    public string? AfterColumn { get; set; }

    /// <summary>
    /// Whether to add the column first.
    /// </summary>
    public bool IsFirst { get; set; }
}

/// <summary>
/// DROP COLUMN action.
/// </summary>
public class DropColumnAction : AlterTableAction
{
    /// <summary>
    /// The column name to drop.
    /// </summary>
    public string ColumnName { get; set; } = null!;
}

/// <summary>
/// MODIFY COLUMN action (change column definition but keep name).
/// </summary>
public class ModifyColumnAction : AlterTableAction
{
    /// <summary>
    /// The new column definition (Name contains existing column name).
    /// </summary>
    public ColumnDef Column { get; set; } = null!;

    /// <summary>
    /// Optional position - FIRST or AFTER column_name.
    /// </summary>
    public string? AfterColumn { get; set; }

    /// <summary>
    /// Whether to move the column first.
    /// </summary>
    public bool IsFirst { get; set; }
}

/// <summary>
/// CHANGE COLUMN action (rename and/or change definition).
/// </summary>
public class ChangeColumnAction : AlterTableAction
{
    /// <summary>
    /// The old column name.
    /// </summary>
    public string OldColumnName { get; set; } = null!;

    /// <summary>
    /// The new column definition (Name contains new column name).
    /// </summary>
    public ColumnDef NewColumn { get; set; } = null!;

    /// <summary>
    /// Optional position - FIRST or AFTER column_name.
    /// </summary>
    public string? AfterColumn { get; set; }

    /// <summary>
    /// Whether to move the column first.
    /// </summary>
    public bool IsFirst { get; set; }
}

/// <summary>
/// RENAME COLUMN action.
/// </summary>
public class RenameColumnAction : AlterTableAction
{
    /// <summary>
    /// The old column name.
    /// </summary>
    public string OldName { get; set; } = null!;

    /// <summary>
    /// The new column name.
    /// </summary>
    public string NewName { get; set; } = null!;
}

/// <summary>
/// RENAME TABLE action.
/// </summary>
public class RenameTableAction : AlterTableAction
{
    /// <summary>
    /// The new table name.
    /// </summary>
    public string NewName { get; set; } = null!;
}

/// <summary>
/// ADD INDEX action.
/// </summary>
public class AddIndexAction : AlterTableAction
{
    /// <summary>
    /// The index name (optional - auto-generated if null).
    /// </summary>
    public string? IndexName { get; set; }

    /// <summary>
    /// The columns to index.
    /// </summary>
    public List<string> Columns { get; set; } = [];

    /// <summary>
    /// Whether this is a UNIQUE index.
    /// </summary>
    public bool IsUnique { get; set; }
}

/// <summary>
/// DROP INDEX action.
/// </summary>
public class DropIndexAction : AlterTableAction
{
    /// <summary>
    /// The index name.
    /// </summary>
    public string IndexName { get; set; } = null!;
}

/// <summary>
/// ADD CONSTRAINT action.
/// </summary>
public class AddConstraintAction : AlterTableAction
{
    /// <summary>
    /// The constraint to add.
    /// </summary>
    public TableConstraint Constraint { get; set; } = null!;
}

/// <summary>
/// DROP CONSTRAINT action.
/// </summary>
public class DropConstraintAction : AlterTableAction
{
    /// <summary>
    /// The constraint name.
    /// </summary>
    public string ConstraintName { get; set; } = null!;
}

/// <summary>
/// ADD PRIMARY KEY action.
/// </summary>
public class AddPrimaryKeyAction : AlterTableAction
{
    /// <summary>
    /// The columns for the primary key.
    /// </summary>
    public List<string> Columns { get; set; } = [];
}

/// <summary>
/// DROP PRIMARY KEY action.
/// </summary>
public class DropPrimaryKeyAction : AlterTableAction
{
}

/// <summary>
/// ADD FOREIGN KEY action.
/// </summary>
public class AddForeignKeyAction : AlterTableAction
{
    /// <summary>
    /// The constraint name (optional).
    /// </summary>
    public string? ConstraintName { get; set; }

    /// <summary>
    /// The columns in this table.
    /// </summary>
    public List<string> Columns { get; set; } = [];

    /// <summary>
    /// The referenced table.
    /// </summary>
    public string ReferencedTable { get; set; } = null!;

    /// <summary>
    /// The referenced columns.
    /// </summary>
    public List<string> ReferencedColumns { get; set; } = [];

    /// <summary>
    /// The action to take when the referenced row is deleted.
    /// </summary>
    public ForeignKeyReferentialAction OnDelete { get; set; } = ForeignKeyReferentialAction.Restrict;

    /// <summary>
    /// The action to take when the referenced row is updated.
    /// </summary>
    public ForeignKeyReferentialAction OnUpdate { get; set; } = ForeignKeyReferentialAction.Restrict;
}

/// <summary>
/// DROP FOREIGN KEY action.
/// </summary>
public class DropForeignKeyAction : AlterTableAction
{
    /// <summary>
    /// The foreign key constraint name.
    /// </summary>
    public string ConstraintName { get; set; } = null!;
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
    Hash,
    Fulltext
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

/// <summary>
/// Transaction isolation level.
/// </summary>
public enum TransactionIsolationLevel
{
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable
}

/// <summary>
/// Transaction access mode.
/// </summary>
public enum TransactionAccessMode
{
    ReadWrite,
    ReadOnly
}

/// <summary>
/// Represents a SET TRANSACTION statement.
/// SET TRANSACTION ISOLATION LEVEL level [, READ ONLY | READ WRITE]
/// </summary>
public class SetTransactionStatement : Statement
{
    /// <summary>
    /// The isolation level (null if not specified).
    /// </summary>
    public TransactionIsolationLevel? IsolationLevel { get; set; }

    /// <summary>
    /// The access mode (null if not specified).
    /// </summary>
    public TransactionAccessMode? AccessMode { get; set; }

    /// <summary>
    /// Whether this applies to the next transaction only (SESSION) or all future transactions (GLOBAL).
    /// </summary>
    public SetScope Scope { get; set; } = SetScope.Session;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitSetTransactionStatement(this);
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

#region User Management Statements

/// <summary>
/// Represents a CREATE USER statement.
/// </summary>
public class CreateUserStatement : Statement
{
    /// <summary>
    /// The username.
    /// </summary>
    public string UserName { get; set; } = null!;

    /// <summary>
    /// The host (default is '%').
    /// </summary>
    public string Host { get; set; } = "%";

    /// <summary>
    /// The password (optional).
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Whether IF NOT EXISTS was specified.
    /// </summary>
    public bool IfNotExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateUserStatement(this);
}

/// <summary>
/// Represents an ALTER USER statement.
/// </summary>
public class AlterUserStatement : Statement
{
    /// <summary>
    /// The username.
    /// </summary>
    public string UserName { get; set; } = null!;

    /// <summary>
    /// The host.
    /// </summary>
    public string Host { get; set; } = "%";

    /// <summary>
    /// The new password (optional).
    /// </summary>
    public string? Password { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitAlterUserStatement(this);
}

/// <summary>
/// Represents a DROP USER statement.
/// </summary>
public class DropUserStatement : Statement
{
    /// <summary>
    /// The username.
    /// </summary>
    public string UserName { get; set; } = null!;

    /// <summary>
    /// The host.
    /// </summary>
    public string Host { get; set; } = "%";

    /// <summary>
    /// Whether IF EXISTS was specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropUserStatement(this);
}

/// <summary>
/// Represents a GRANT statement.
/// </summary>
public class GrantStatement : Statement
{
    /// <summary>
    /// The privileges to grant (e.g., SELECT, INSERT, ALL PRIVILEGES).
    /// </summary>
    public List<string> Privileges { get; set; } = [];

    /// <summary>
    /// The database name (null for all databases).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// The table name (null for all tables).
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// The username to grant to.
    /// </summary>
    public string UserName { get; set; } = null!;

    /// <summary>
    /// The host.
    /// </summary>
    public string Host { get; set; } = "%";

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitGrantStatement(this);
}

/// <summary>
/// Represents a REVOKE statement.
/// </summary>
public class RevokeStatement : Statement
{
    /// <summary>
    /// The privileges to revoke.
    /// </summary>
    public List<string> Privileges { get; set; } = [];

    /// <summary>
    /// The database name (null for all databases).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// The table name (null for all tables).
    /// </summary>
    public string? TableName { get; set; }

    /// <summary>
    /// The username to revoke from.
    /// </summary>
    public string UserName { get; set; } = null!;

    /// <summary>
    /// The host.
    /// </summary>
    public string Host { get; set; } = "%";

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitRevokeStatement(this);
}

#endregion

#region Stored Procedures, Functions, Triggers, Events

/// <summary>
/// Parameter mode for stored procedures/functions.
/// </summary>
public enum ParameterMode
{
    In,
    Out,
    InOut
}

/// <summary>
/// Represents a parameter for a stored procedure or function.
/// </summary>
public class ProcedureParameter
{
    /// <summary>
    /// The parameter name.
    /// </summary>
    public string Name { get; set; } = null!;

    /// <summary>
    /// The parameter mode (IN, OUT, INOUT).
    /// </summary>
    public ParameterMode Mode { get; set; } = ParameterMode.In;

    /// <summary>
    /// The parameter data type.
    /// </summary>
    public DataType DataType { get; set; }

    /// <summary>
    /// Size/precision for the data type (for VARCHAR, DECIMAL, etc.).
    /// </summary>
    public int? Size { get; set; }

    /// <summary>
    /// Scale for DECIMAL types.
    /// </summary>
    public int? Scale { get; set; }
}

/// <summary>
/// Represents a CREATE PROCEDURE statement.
/// </summary>
public class CreateProcedureStatement : Statement
{
    /// <summary>
    /// The procedure name.
    /// </summary>
    public string ProcedureName { get; set; } = null!;

    /// <summary>
    /// The parameters for the procedure.
    /// </summary>
    public List<ProcedureParameter> Parameters { get; set; } = [];

    /// <summary>
    /// The procedure body (list of statements).
    /// </summary>
    public List<Statement> Body { get; set; } = [];

    /// <summary>
    /// The definer (user@host).
    /// </summary>
    public string? Definer { get; set; }

    /// <summary>
    /// SQL SECURITY (DEFINER or INVOKER).
    /// </summary>
    public string? SqlSecurity { get; set; }

    /// <summary>
    /// Comment for the procedure.
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>
    /// Whether OR REPLACE was specified.
    /// </summary>
    public bool OrReplace { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateProcedureStatement(this);
}

/// <summary>
/// Represents a DROP PROCEDURE statement.
/// </summary>
public class DropProcedureStatement : Statement
{
    /// <summary>
    /// The procedure name to drop.
    /// </summary>
    public string ProcedureName { get; set; } = null!;

    /// <summary>
    /// Whether IF EXISTS was specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropProcedureStatement(this);
}

/// <summary>
/// Represents a CALL statement to execute a stored procedure.
/// </summary>
public class CallStatement : Statement
{
    /// <summary>
    /// The procedure name to call.
    /// </summary>
    public string ProcedureName { get; set; } = null!;

    /// <summary>
    /// The arguments to pass to the procedure.
    /// </summary>
    public List<Expression> Arguments { get; set; } = [];

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCallStatement(this);
}

/// <summary>
/// Represents a DECLARE variable statement within a procedure.
/// </summary>
public class DeclareVariableStatement : Statement
{
    /// <summary>
    /// The variable names to declare.
    /// </summary>
    public List<string> VariableNames { get; set; } = [];

    /// <summary>
    /// The data type for the variables.
    /// </summary>
    public DataType DataType { get; set; }

    /// <summary>
    /// Size/precision for the data type.
    /// </summary>
    public int? Size { get; set; }

    /// <summary>
    /// Scale for DECIMAL types.
    /// </summary>
    public int? Scale { get; set; }

    /// <summary>
    /// Default value for the variables.
    /// </summary>
    public Expression? DefaultValue { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDeclareVariableStatement(this);
}

/// <summary>
/// Represents an IF statement within a procedure.
/// </summary>
public class IfStatement : Statement
{
    /// <summary>
    /// The condition to test.
    /// </summary>
    public Expression Condition { get; set; } = null!;

    /// <summary>
    /// The statements to execute if the condition is true.
    /// </summary>
    public List<Statement> ThenStatements { get; set; } = [];

    /// <summary>
    /// ELSEIF clauses.
    /// </summary>
    public List<(Expression Condition, List<Statement> Statements)> ElseIfClauses { get; set; } = [];

    /// <summary>
    /// The ELSE statements (executed if all conditions are false).
    /// </summary>
    public List<Statement>? ElseStatements { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitIfStatement(this);
}

/// <summary>
/// Represents a WHILE loop statement.
/// </summary>
public class WhileStatement : Statement
{
    /// <summary>
    /// The loop condition.
    /// </summary>
    public Expression Condition { get; set; } = null!;

    /// <summary>
    /// The loop body.
    /// </summary>
    public List<Statement> Body { get; set; } = [];

    /// <summary>
    /// Optional label for the loop.
    /// </summary>
    public string? Label { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitWhileStatement(this);
}

/// <summary>
/// Represents a REPEAT loop statement.
/// </summary>
public class RepeatStatement : Statement
{
    /// <summary>
    /// The loop body.
    /// </summary>
    public List<Statement> Body { get; set; } = [];

    /// <summary>
    /// The loop condition (loop continues UNTIL this is true).
    /// </summary>
    public Expression UntilCondition { get; set; } = null!;

    /// <summary>
    /// Optional label for the loop.
    /// </summary>
    public string? Label { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitRepeatStatement(this);
}

/// <summary>
/// Represents a LOOP statement (infinite loop with LEAVE/ITERATE).
/// </summary>
public class LoopStatement : Statement
{
    /// <summary>
    /// The loop body.
    /// </summary>
    public List<Statement> Body { get; set; } = [];

    /// <summary>
    /// The label for the loop (required for LOOP).
    /// </summary>
    public string Label { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitLoopStatement(this);
}

/// <summary>
/// Represents a LEAVE statement (exit a loop or block).
/// </summary>
public class LeaveStatement : Statement
{
    /// <summary>
    /// The label to leave.
    /// </summary>
    public string Label { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitLeaveStatement(this);
}

/// <summary>
/// Represents an ITERATE statement (continue to next loop iteration).
/// </summary>
public class IterateStatement : Statement
{
    /// <summary>
    /// The label to iterate.
    /// </summary>
    public string Label { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitIterateStatement(this);
}

/// <summary>
/// Represents a RETURN statement in a stored function.
/// </summary>
public class ReturnStatement : Statement
{
    /// <summary>
    /// The value to return.
    /// </summary>
    public Expression Value { get; set; } = null!;

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitReturnStatement(this);
}

/// <summary>
/// Represents a CREATE FUNCTION statement.
/// </summary>
public class CreateFunctionStatement : Statement
{
    /// <summary>
    /// The function name.
    /// </summary>
    public string FunctionName { get; set; } = null!;

    /// <summary>
    /// The parameters for the function.
    /// </summary>
    public List<ProcedureParameter> Parameters { get; set; } = [];

    /// <summary>
    /// The return data type.
    /// </summary>
    public DataType ReturnType { get; set; }

    /// <summary>
    /// Size/precision for the return type.
    /// </summary>
    public int? ReturnSize { get; set; }

    /// <summary>
    /// Scale for DECIMAL return types.
    /// </summary>
    public int? ReturnScale { get; set; }

    /// <summary>
    /// The function body.
    /// </summary>
    public List<Statement> Body { get; set; } = [];

    /// <summary>
    /// Whether the function is DETERMINISTIC.
    /// </summary>
    public bool IsDeterministic { get; set; }

    /// <summary>
    /// The definer (user@host).
    /// </summary>
    public string? Definer { get; set; }

    /// <summary>
    /// SQL SECURITY (DEFINER or INVOKER).
    /// </summary>
    public string? SqlSecurity { get; set; }

    /// <summary>
    /// Comment for the function.
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>
    /// Whether OR REPLACE was specified.
    /// </summary>
    public bool OrReplace { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateFunctionStatement(this);
}

/// <summary>
/// Represents a DROP FUNCTION statement.
/// </summary>
public class DropFunctionStatement : Statement
{
    /// <summary>
    /// The function name to drop.
    /// </summary>
    public string FunctionName { get; set; } = null!;

    /// <summary>
    /// Whether IF EXISTS was specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropFunctionStatement(this);
}

/// <summary>
/// Trigger timing (BEFORE or AFTER).
/// </summary>
public enum TriggerTiming
{
    Before,
    After
}

/// <summary>
/// Trigger event (INSERT, UPDATE, DELETE).
/// </summary>
public enum TriggerEvent
{
    Insert,
    Update,
    Delete
}

/// <summary>
/// Represents a CREATE TRIGGER statement.
/// </summary>
public class CreateTriggerStatement : Statement
{
    /// <summary>
    /// The trigger name.
    /// </summary>
    public string TriggerName { get; set; } = null!;

    /// <summary>
    /// The timing (BEFORE or AFTER).
    /// </summary>
    public TriggerTiming Timing { get; set; }

    /// <summary>
    /// The event (INSERT, UPDATE, DELETE).
    /// </summary>
    public TriggerEvent Event { get; set; }

    /// <summary>
    /// The table name the trigger is on.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The trigger body.
    /// </summary>
    public List<Statement> Body { get; set; } = [];

    /// <summary>
    /// The definer (user@host).
    /// </summary>
    public string? Definer { get; set; }

    /// <summary>
    /// Whether OR REPLACE was specified.
    /// </summary>
    public bool OrReplace { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateTriggerStatement(this);
}

/// <summary>
/// Represents a DROP TRIGGER statement.
/// </summary>
public class DropTriggerStatement : Statement
{
    /// <summary>
    /// The trigger name to drop.
    /// </summary>
    public string TriggerName { get; set; } = null!;

    /// <summary>
    /// Whether IF EXISTS was specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropTriggerStatement(this);
}

/// <summary>
/// Represents a CREATE EVENT statement.
/// </summary>
public class CreateEventStatement : Statement
{
    /// <summary>
    /// The event name.
    /// </summary>
    public string EventName { get; set; } = null!;

    /// <summary>
    /// The schedule expression (e.g., "EVERY 1 DAY", "AT '2024-01-01 00:00:00'").
    /// </summary>
    public string Schedule { get; set; } = null!;

    /// <summary>
    /// The event body.
    /// </summary>
    public List<Statement> Body { get; set; } = [];

    /// <summary>
    /// Whether ON COMPLETION PRESERVE is specified.
    /// </summary>
    public bool OnCompletionPreserve { get; set; }

    /// <summary>
    /// Whether the event is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Comment for the event.
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>
    /// The definer (user@host).
    /// </summary>
    public string? Definer { get; set; }

    /// <summary>
    /// Whether OR REPLACE was specified.
    /// </summary>
    public bool OrReplace { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitCreateEventStatement(this);
}

/// <summary>
/// Represents a DROP EVENT statement.
/// </summary>
public class DropEventStatement : Statement
{
    /// <summary>
    /// The event name to drop.
    /// </summary>
    public string EventName { get; set; } = null!;

    /// <summary>
    /// Whether IF EXISTS was specified.
    /// </summary>
    public bool IfExists { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitDropEventStatement(this);
}

#endregion

#region Admin Statements

/// <summary>
/// Represents an ANALYZE TABLE statement.
/// </summary>
public class AnalyzeTableStatement : Statement
{
    /// <summary>
    /// The table names to analyze.
    /// </summary>
    public List<string> TableNames { get; set; } = [];

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitAnalyzeTableStatement(this);
}

/// <summary>
/// Represents a FLUSH statement.
/// </summary>
public class FlushStatement : Statement
{
    /// <summary>
    /// The type of flush (TABLES, PRIVILEGES, LOGS, etc.).
    /// </summary>
    public string FlushType { get; set; } = null!;

    /// <summary>
    /// Optional table names for FLUSH TABLES.
    /// </summary>
    public List<string> TableNames { get; set; } = [];

    /// <summary>
    /// Whether WITH READ LOCK is specified.
    /// </summary>
    public bool WithReadLock { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitFlushStatement(this);
}

/// <summary>
/// Represents a LOCK TABLES statement.
/// </summary>
public class LockTablesStatement : Statement
{
    /// <summary>
    /// The table locks to acquire.
    /// </summary>
    public List<TableLock> TableLocks { get; set; } = [];

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitLockTablesStatement(this);
}

/// <summary>
/// Represents a single table lock specification.
/// </summary>
public class TableLock
{
    /// <summary>
    /// The table name.
    /// </summary>
    public string TableName { get; set; } = null!;

    /// <summary>
    /// The lock type (READ, WRITE, READ LOCAL, LOW PRIORITY WRITE).
    /// </summary>
    public string LockType { get; set; } = null!;

    /// <summary>
    /// Optional alias for the table.
    /// </summary>
    public string? Alias { get; set; }
}

/// <summary>
/// Represents an UNLOCK TABLES statement.
/// </summary>
public class UnlockTablesStatement : Statement
{
    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitUnlockTablesStatement(this);
}

#endregion

#region EXPLAIN Statements

/// <summary>
/// Represents an EXPLAIN statement.
/// </summary>
public class ExplainStatement : Statement
{
    /// <summary>
    /// The statement to explain.
    /// </summary>
    public Statement Statement { get; set; } = null!;

    /// <summary>
    /// The explain format (TRADITIONAL, JSON, TREE).
    /// </summary>
    public ExplainFormat Format { get; set; } = ExplainFormat.Traditional;

    /// <summary>
    /// Whether ANALYZE is specified (actually executes and shows real stats).
    /// </summary>
    public bool Analyze { get; set; }

    public override T Accept<T>(IAstVisitor<T> visitor) => visitor.VisitExplainStatement(this);
}

/// <summary>
/// EXPLAIN output format.
/// </summary>
public enum ExplainFormat
{
    Traditional,
    Json,
    Tree
}

#endregion
