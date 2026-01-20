namespace CyscaleDB.Core.Parsing.Ast;

/// <summary>
/// Base class for all AST nodes.
/// </summary>
public abstract class AstNode
{
    /// <summary>
    /// Accepts a visitor for traversing the AST.
    /// </summary>
    public abstract T Accept<T>(IAstVisitor<T> visitor);
}

/// <summary>
/// Visitor interface for AST traversal.
/// </summary>
public interface IAstVisitor<T>
{
    T VisitSelectStatement(SelectStatement node);
    T VisitInsertStatement(InsertStatement node);
    T VisitUpdateStatement(UpdateStatement node);
    T VisitDeleteStatement(DeleteStatement node);
    T VisitCreateTableStatement(CreateTableStatement node);
    T VisitDropTableStatement(DropTableStatement node);
    T VisitCreateDatabaseStatement(CreateDatabaseStatement node);
    T VisitDropDatabaseStatement(DropDatabaseStatement node);
    T VisitUseDatabaseStatement(UseDatabaseStatement node);
    T VisitShowTablesStatement(ShowTablesStatement node);
    T VisitShowDatabasesStatement(ShowDatabasesStatement node);
    T VisitDescribeStatement(DescribeStatement node);
    T VisitBeginStatement(BeginStatement node);
    T VisitCommitStatement(CommitStatement node);
    T VisitRollbackStatement(RollbackStatement node);
    
    T VisitBinaryExpression(BinaryExpression node);
    T VisitUnaryExpression(UnaryExpression node);
    T VisitLiteralExpression(LiteralExpression node);
    T VisitColumnReference(ColumnReference node);
    T VisitFunctionCall(FunctionCall node);
    T VisitSubquery(Subquery node);
    T VisitInExpression(InExpression node);
    T VisitBetweenExpression(BetweenExpression node);
    T VisitIsNullExpression(IsNullExpression node);
    T VisitLikeExpression(LikeExpression node);
    T VisitExistsExpression(ExistsExpression node);
    T VisitColumnDefinitionNode(ColumnDefinitionNode node);
}
