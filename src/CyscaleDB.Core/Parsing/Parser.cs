using CyscaleDB.Core.Common;
using CyscaleDB.Core.Parsing.Ast;

namespace CyscaleDB.Core.Parsing;

/// <summary>
/// SQL parser using recursive descent parsing.
/// </summary>
public sealed class Parser
{
    private readonly Lexer _lexer;
    private Token _currentToken;

    /// <summary>
    /// Creates a new parser for the given SQL string.
    /// </summary>
    public Parser(string sql)
    {
        _lexer = new Lexer(sql);
        _currentToken = _lexer.NextToken();
    }

    /// <summary>
    /// Parses a single SQL statement.
    /// </summary>
    public Statement Parse()
    {
        var statement = ParseStatement();
        
        // Consume optional semicolon
        if (Check(TokenType.Semicolon))
        {
            Advance();
        }

        // Expect EOF
        if (!Check(TokenType.EOF))
        {
            throw Error($"Unexpected token: {_currentToken.Value}");
        }

        return statement;
    }

    /// <summary>
    /// Parses multiple SQL statements.
    /// </summary>
    public List<Statement> ParseMultiple()
    {
        var statements = new List<Statement>();

        while (!Check(TokenType.EOF))
        {
            // Skip whitespace and comments before parsing next statement
            // (Lexer already does this, but ensure we're at a valid statement start)
            if (Check(TokenType.WHERE) || Check(TokenType.GROUP) || Check(TokenType.ORDER) || 
                Check(TokenType.HAVING) || Check(TokenType.LIMIT) || Check(TokenType.OFFSET))
            {
                throw Error($"Unexpected {_currentToken.Value} clause. This clause cannot appear at the start of a statement. " +
                           $"It may be part of a previous incomplete statement or a syntax error.");
            }

            statements.Add(ParseStatement());
            
            // Consume optional semicolon
            while (Check(TokenType.Semicolon))
            {
                Advance();
            }
        }

        return statements;
    }

    /// <summary>
    /// Parses a single expression (e.g., for CHECK constraint evaluation).
    /// </summary>
    public Expression ParseSingleExpression()
    {
        var expr = ParseExpression();
        
        // Expect EOF
        if (!Check(TokenType.EOF))
        {
            throw Error($"Unexpected token after expression: {_currentToken.Value}");
        }

        return expr;
    }

    private Statement ParseStatement()
    {
        return _currentToken.Type switch
        {
            TokenType.WITH => ParseWithStatement(),
            TokenType.SELECT => ParseSelectStatement(),
            TokenType.UNION => throw Error("UNION cannot appear at the start of a statement. Use SELECT ... UNION SELECT ..."),
            TokenType.INSERT => ParseInsertStatement(),
            TokenType.UPDATE => ParseUpdateStatement(),
            TokenType.DELETE => ParseDeleteStatement(),
            TokenType.CREATE => ParseCreateStatement(),
            TokenType.DROP => ParseDropStatement(),
            TokenType.ALTER => ParseAlterStatement(),
            TokenType.USE => ParseUseStatement(),
            TokenType.SHOW => ParseShowStatement(),
            TokenType.DESCRIBE => ParseDescribeStatement(),
            TokenType.BEGIN => ParseBeginStatement(),
            TokenType.START => ParseStartStatement(),
            TokenType.COMMIT => ParseCommitStatement(),
            TokenType.ROLLBACK => ParseRollbackStatement(),
            TokenType.OPTIMIZE => ParseOptimizeStatement(),
            TokenType.SET => ParseSetStatement(),
            TokenType.KILL => ParseKillStatement(),
            TokenType.GRANT => ParseGrantStatement(),
            TokenType.REVOKE => ParseRevokeStatement(),
            TokenType.CALL => ParseCallStatement(),
            // Stored procedure control flow statements (only valid inside procedures)
            TokenType.DECLARE => ParseDeclareVariableStatement(),
            TokenType.IF => ParseIfStatement(),
            TokenType.WHILE => ParseWhileStatement(),
            TokenType.REPEAT => ParseRepeatStatement(),
            TokenType.LOOP => ParseLoopStatement(),
            TokenType.LEAVE => ParseLeaveStatement(),
            TokenType.ITERATE => ParseIterateStatement(),
            TokenType.RETURN => ParseReturnStatement(),
            // Admin statements
            TokenType.ANALYZE => ParseAnalyzeStatement(),
            TokenType.FLUSH => ParseFlushStatement(),
            TokenType.LOCK => ParseLockTablesStatement(),
            TokenType.UNLOCK => ParseUnlockTablesStatement(),
            TokenType.EXPLAIN => ParseExplainStatement(),
            _ => throw Error($"Unexpected token at start of statement: {_currentToken.Value}")
        };
    }

    /// <summary>
    /// Parses a statement starting with WITH (CTE).
    /// </summary>
    private SelectStatement ParseWithStatement()
    {
        var withClause = ParseWithClause();
        var stmt = ParseSelectStatement();
        stmt.WithClause = withClause;
        return stmt;
    }

    /// <summary>
    /// Parses a WITH clause containing one or more CTEs.
    /// Syntax: WITH [RECURSIVE] cte_name [(column_list)] AS (subquery) [, ...]
    /// </summary>
    private WithClause ParseWithClause()
    {
        Expect(TokenType.WITH);
        
        var withClause = new WithClause();
        
        // Check for RECURSIVE
        if (Match(TokenType.RECURSIVE))
        {
            withClause.IsRecursive = true;
        }

        // Parse CTE definitions
        do
        {
            var cte = new CteDefinition();
            
            // CTE name
            cte.Name = ExpectIdentifier();

            // Optional column list
            if (Match(TokenType.LeftParen))
            {
                cte.Columns = ParseIdentifierList();
                Expect(TokenType.RightParen);
            }

            // AS keyword
            Expect(TokenType.AS);

            // Subquery in parentheses
            Expect(TokenType.LeftParen);
            cte.Query = ParseSelectStatement();
            Expect(TokenType.RightParen);

            withClause.Ctes.Add(cte);
        }
        while (Match(TokenType.Comma));

        return withClause;
    }

    private OptimizeTableStatement ParseOptimizeStatement()
    {
        Expect(TokenType.OPTIMIZE);
        Expect(TokenType.TABLE);

        var stmt = new OptimizeTableStatement();
        stmt.TableName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        return stmt;
    }

    #region SELECT Statement

    private SelectStatement ParseSelectStatement()
    {
        Expect(TokenType.SELECT);
        var stmt = new SelectStatement();

        // DISTINCT
        if (Check(TokenType.DISTINCT))
        {
            Advance();
            stmt.IsDistinct = true;
        }
        else if (Check(TokenType.ALL))
        {
            Advance();
        }

        // Column list
        stmt.Columns = ParseSelectColumns();

        // FROM clause
        if (Check(TokenType.FROM))
        {
            Advance();
            stmt.From = ParseTableReference();
        }

        // WHERE clause
        if (Check(TokenType.WHERE))
        {
            Advance();
            stmt.Where = ParseExpression();
        }

        // GROUP BY clause
        if (Check(TokenType.GROUP))
        {
            Advance();
            Expect(TokenType.BY);
            stmt.GroupBy = ParseExpressionList();
        }

        // HAVING clause
        if (Check(TokenType.HAVING))
        {
            Advance();
            stmt.Having = ParseExpression();
        }

        // ORDER BY clause
        if (Check(TokenType.ORDER))
        {
            Advance();
            Expect(TokenType.BY);
            stmt.OrderBy = ParseOrderByList();
        }

        // LIMIT clause - supports both "LIMIT count [OFFSET offset]" and "LIMIT offset, count"
        if (Check(TokenType.LIMIT))
        {
            Advance();
            var firstNum = ParseInteger();

            if (Match(TokenType.Comma))
            {
                // MySQL syntax: LIMIT offset, count (offset comes first!)
                stmt.Offset = firstNum;
                stmt.Limit = ParseInteger();
            }
            else
            {
                stmt.Limit = firstNum;
                // OFFSET clause
                if (Check(TokenType.OFFSET))
                {
                    Advance();
                    stmt.Offset = ParseInteger();
                }
            }
        }

        // FOR UPDATE / FOR SHARE clause
        ParseLockingClause(stmt);

        // Set operations (UNION, INTERSECT, EXCEPT)
        while (Check(TokenType.UNION) || Check(TokenType.INTERSECT) || Check(TokenType.EXCEPT))
        {
            SetOperationType opType;
            if (Check(TokenType.UNION))
            {
                Advance();
                opType = SetOperationType.Union;
            }
            else if (Check(TokenType.INTERSECT))
            {
                Advance();
                opType = SetOperationType.Intersect;
            }
            else // EXCEPT
            {
                Advance();
                opType = SetOperationType.Except;
            }

            bool hasAll = Match(TokenType.ALL);
            
            // Add to new SetOperation collections
            stmt.SetOperationTypes.Add(opType);
            stmt.SetOperationAllFlags.Add(hasAll);
            
            // Also add to legacy UnionAllFlags for backward compatibility
            stmt.UnionAllFlags.Add(hasAll);
            
            // Parse the next SELECT statement
            Expect(TokenType.SELECT);
            var unionStmt = new SelectStatement();
            
            // DISTINCT
            if (Check(TokenType.DISTINCT))
            {
                Advance();
                unionStmt.IsDistinct = true;
            }
            else if (Check(TokenType.ALL))
            {
                Advance();
            }

            // Column list
            unionStmt.Columns = ParseSelectColumns();

            // FROM clause
            if (Check(TokenType.FROM))
            {
                Advance();
                unionStmt.From = ParseTableReference();
            }

            // WHERE clause
            if (Check(TokenType.WHERE))
            {
                Advance();
                unionStmt.Where = ParseExpression();
            }

            // GROUP BY clause
            if (Check(TokenType.GROUP))
            {
                Advance();
                Expect(TokenType.BY);
                unionStmt.GroupBy = ParseExpressionList();
            }

            // HAVING clause
            if (Check(TokenType.HAVING))
            {
                Advance();
                unionStmt.Having = ParseExpression();
            }

            // ORDER BY clause (only allowed on the last UNION query)
            if (Check(TokenType.ORDER))
            {
                Advance();
                Expect(TokenType.BY);
                unionStmt.OrderBy = ParseOrderByList();
            }

            // LIMIT clause (only allowed on the last UNION query)
            // Supports both "LIMIT count [OFFSET offset]" and "LIMIT offset, count"
            if (Check(TokenType.LIMIT))
            {
                Advance();
                var firstNum = ParseInteger();

                if (Match(TokenType.Comma))
                {
                    // MySQL syntax: LIMIT offset, count (offset comes first!)
                    unionStmt.Offset = firstNum;
                    unionStmt.Limit = ParseInteger();
                }
                else
                {
                    unionStmt.Limit = firstNum;
                    if (Check(TokenType.OFFSET))
                    {
                        Advance();
                        unionStmt.Offset = ParseInteger();
                    }
                }
            }

            // Add to both new and legacy collections
            stmt.SetOperationQueries.Add(unionStmt);
            stmt.UnionQueries.Add(unionStmt);
        }

        return stmt;
    }

    /// <summary>
    /// Parses FOR UPDATE / FOR SHARE locking clause.
    /// Syntax:
    ///   FOR UPDATE [OF table_list] [NOWAIT | SKIP LOCKED]
    ///   FOR SHARE [OF table_list] [NOWAIT | SKIP LOCKED]
    ///   LOCK IN SHARE MODE
    /// </summary>
    private void ParseLockingClause(SelectStatement stmt)
    {
        // MySQL/MariaDB syntax: LOCK IN SHARE MODE (legacy)
        if (Check(TokenType.LOCK))
        {
            Advance();
            Expect(TokenType.IN);
            Expect(TokenType.SHARE);
            Expect(TokenType.MODE);
            stmt.LockMode = SelectLockMode.ForShare;
            return;
        }

        // Standard syntax: FOR UPDATE / FOR SHARE
        if (!Check(TokenType.FOR))
            return;

        Advance(); // consume FOR

        if (Match(TokenType.UPDATE))
        {
            stmt.LockMode = SelectLockMode.ForUpdate;
        }
        else if (Match(TokenType.SHARE))
        {
            stmt.LockMode = SelectLockMode.ForShare;
        }
        else
        {
            throw Error("Expected UPDATE or SHARE after FOR");
        }

        // Optional: OF table_list
        if (MatchIdentifier("OF"))
        {
            do
            {
                stmt.LockTables.Add(ExpectIdentifier());
            } while (Match(TokenType.Comma));
        }

        // Optional: NOWAIT or SKIP LOCKED
        if (Match(TokenType.NOWAIT))
        {
            stmt.NoWait = true;
        }
        else if (Match(TokenType.SKIP))
        {
            Expect(TokenType.LOCKED);
            stmt.SkipLocked = true;
        }
    }

    private List<SelectColumn> ParseSelectColumns()
    {
        var columns = new List<SelectColumn>();

        do
        {
            columns.Add(ParseSelectColumn());
        } while (Match(TokenType.Comma));

        return columns;
    }

    private SelectColumn ParseSelectColumn()
    {
        var column = new SelectColumn();

        // Check for * or table.*
        if (Check(TokenType.Asterisk))
        {
            Advance();
            column.IsWildcard = true;
            column.Expression = new LiteralExpression { Value = DataValue.Null }; // Placeholder
            return column;
        }

        // Check for expression-starting keywords that should be parsed as expressions
        // (CASE, NOT, EXISTS, etc.)
        if (Check(TokenType.CASE) || Check(TokenType.NOT) || Check(TokenType.EXISTS) ||
            Check(TokenType.LeftParen) || Check(TokenType.Minus) || Check(TokenType.Plus) ||
            Check(TokenType.IntegerLiteral) || Check(TokenType.FloatLiteral) ||
            Check(TokenType.StringLiteral) || Check(TokenType.NULL) ||
            Check(TokenType.TRUE) || Check(TokenType.FALSE) || Check(TokenType.AtAt))
        {
            column.Expression = ParseExpression();

            // Handle alias
            if (Match(TokenType.AS))
            {
                column.Alias = ExpectIdentifier();
            }
            else if (Check(TokenType.Identifier) && !IsKeyword(_currentToken.Type))
            {
                column.Alias = ExpectIdentifier();
            }

            return column;
        }

        // Accept identifier or keyword (keywords can be used as column/table names)
        if (Check(TokenType.Identifier) || IsKeyword(_currentToken.Type))
        {
            var name = _currentToken.Value;
            Advance();

            if (Match(TokenType.Dot))
            {
                if (Check(TokenType.Asterisk))
                {
                    Advance();
                    column.IsWildcard = true;
                    column.TableQualifier = name;
                    column.Expression = new LiteralExpression { Value = DataValue.Null };
                    return column;
                }

                // table.column
                var columnName = ExpectIdentifier();
                column.Expression = new ColumnReference { TableName = name, ColumnName = columnName };
            }
            else
            {
                // Could be column name or function call
                if (Check(TokenType.LeftParen))
                {
                    // Function call
                    column.Expression = ParseFunctionCall(name);
                }
                else
                {
                    column.Expression = new ColumnReference { ColumnName = name };
                }
            }
        }
        else
        {
            // Complex expression
            column.Expression = ParseExpression();
        }

        // AS alias
        if (Match(TokenType.AS))
        {
            column.Alias = ExpectIdentifier();
        }
        else if (Check(TokenType.Identifier) && !IsKeyword(_currentToken.Type))
        {
            column.Alias = ExpectIdentifier();
        }

        return column;
    }

    private List<OrderByClause> ParseOrderByList()
    {
        var list = new List<OrderByClause>();

        do
        {
            var clause = new OrderByClause
            {
                Expression = ParseExpression()
            };

            if (Match(TokenType.DESC))
            {
                clause.Descending = true;
            }
            else
            {
                Match(TokenType.ASC); // Optional ASC
            }

            list.Add(clause);
        } while (Match(TokenType.Comma));

        return list;
    }

    #endregion

    #region Table References

    private TableReference ParseTableReference()
    {
        var left = ParseSimpleTableReference();

        // Parse JOINs
        while (IsJoinKeyword())
        {
            bool isNatural = false;
            List<string> usingColumns = [];

            // Check for NATURAL keyword before join type
            if (Check(TokenType.NATURAL))
            {
                isNatural = true;
            }

            var joinType = ParseJoinType();
            var right = ParseSimpleTableReference();

            Expression? condition = null;
            
            // Parse join condition: ON, USING, or implicit (for NATURAL)
            if (isNatural)
            {
                // NATURAL JOIN: condition is implicit (equality on all common columns)
                // No ON or USING clause allowed
            }
            else if (Match(TokenType.USING))
            {
                // USING clause: equality on specified columns
                Expect(TokenType.LeftParen);
                usingColumns = ParseIdentifierList();
                Expect(TokenType.RightParen);
            }
            else if (Match(TokenType.ON))
            {
                // ON clause: explicit condition
                condition = ParseExpression();
            }

            left = new JoinTableReference
            {
                Left = left,
                Right = right,
                JoinType = joinType,
                Condition = condition,
                IsNatural = isNatural,
                UsingColumns = usingColumns
            };
        }

        return left;
    }

    private TableReference ParseSimpleTableReference()
    {
        // Subquery
        if (Check(TokenType.LeftParen))
        {
            Advance();
            var subquery = ParseSelectStatement();
            Expect(TokenType.RightParen);
            
            string subqueryAlias;
            if (Match(TokenType.AS))
            {
                subqueryAlias = ExpectIdentifier();
            }
            else
            {
                subqueryAlias = ExpectIdentifier();
            }

            return new SubqueryTableReference { Subquery = subquery, Alias = subqueryAlias };
        }

        // Table name
        var tableName = ExpectIdentifier();
        string? dbName = null;

        if (Match(TokenType.Dot))
        {
            dbName = tableName;
            tableName = ExpectIdentifier();
        }

        string? alias = null;
        if (Match(TokenType.AS))
        {
            alias = ExpectIdentifier();
        }
        else if (Check(TokenType.Identifier) && !IsKeyword(_currentToken.Type))
        {
            alias = ExpectIdentifier();
        }

        return new SimpleTableReference
        {
            DatabaseName = dbName,
            TableName = tableName,
            Alias = alias
        };
    }

    private bool IsJoinKeyword()
    {
        return Check(TokenType.JOIN) || Check(TokenType.INNER) || 
               Check(TokenType.LEFT) || Check(TokenType.RIGHT) || 
               Check(TokenType.FULL) || Check(TokenType.CROSS) || 
               Check(TokenType.NATURAL);
    }

    private JoinType ParseJoinType()
    {
        JoinType joinType = JoinType.Inner;

        // Check for NATURAL keyword first
        if (Check(TokenType.NATURAL))
        {
            Advance();
            joinType = JoinType.Natural;
            
            // NATURAL can be combined with INNER/LEFT/RIGHT
            if (Match(TokenType.INNER))
            {
                // NATURAL INNER JOIN
            }
            else if (Match(TokenType.LEFT))
            {
                Match(TokenType.OUTER);
                // NATURAL LEFT JOIN
            }
            else if (Match(TokenType.RIGHT))
            {
                Match(TokenType.OUTER);
                // NATURAL RIGHT JOIN
            }
            // For NATURAL alone, it's treated as NATURAL INNER JOIN
        }
        else if (Match(TokenType.INNER))
        {
            joinType = JoinType.Inner;
        }
        else if (Match(TokenType.LEFT))
        {
            Match(TokenType.OUTER); // Optional OUTER
            joinType = JoinType.Left;
        }
        else if (Match(TokenType.RIGHT))
        {
            Match(TokenType.OUTER);
            joinType = JoinType.Right;
        }
        else if (Match(TokenType.FULL))
        {
            Match(TokenType.OUTER);
            joinType = JoinType.Full;
        }
        else if (Match(TokenType.CROSS))
        {
            joinType = JoinType.Cross;
        }

        Expect(TokenType.JOIN);
        return joinType;
    }

    #endregion

    #region INSERT Statement

    private InsertStatement ParseInsertStatement()
    {
        Expect(TokenType.INSERT);
        Expect(TokenType.INTO);

        var stmt = new InsertStatement();
        stmt.TableName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        // Column list (optional)
        if (Check(TokenType.LeftParen))
        {
            Advance();
            stmt.Columns = ParseIdentifierList();
            Expect(TokenType.RightParen);
        }

        Expect(TokenType.VALUES);

        // Values lists
        do
        {
            Expect(TokenType.LeftParen);
            stmt.ValuesList.Add(ParseExpressionList());
            Expect(TokenType.RightParen);
        } while (Match(TokenType.Comma));

        return stmt;
    }

    #endregion

    #region UPDATE Statement

    private UpdateStatement ParseUpdateStatement()
    {
        Expect(TokenType.UPDATE);

        var stmt = new UpdateStatement();
        stmt.TableName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        Expect(TokenType.SET);

        // SET clauses
        do
        {
            var columnName = ExpectIdentifier();
            Expect(TokenType.Equal);
            var value = ParseExpression();
            stmt.SetClauses.Add(new SetClause { ColumnName = columnName, Value = value });
        } while (Match(TokenType.Comma));

        // WHERE clause
        if (Check(TokenType.WHERE))
        {
            Advance();
            stmt.Where = ParseExpression();
        }

        return stmt;
    }

    #endregion

    #region DELETE Statement

    private DeleteStatement ParseDeleteStatement()
    {
        Expect(TokenType.DELETE);
        Expect(TokenType.FROM);

        var stmt = new DeleteStatement();
        stmt.TableName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        // WHERE clause
        if (Check(TokenType.WHERE))
        {
            Advance();
            stmt.Where = ParseExpression();
        }

        return stmt;
    }

    #endregion

    #region DDL Statements

    private Statement ParseCreateStatement()
    {
        Expect(TokenType.CREATE);

        // Check for UNIQUE or FULLTEXT keyword before INDEX
        bool isUnique = false;
        bool isFulltext = false;
        if (Match(TokenType.UNIQUE))
        {
            isUnique = true;
        }
        else if (Match(TokenType.FULLTEXT))
        {
            isFulltext = true;
        }

        if (Check(TokenType.TABLE))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE TABLE");
            if (isFulltext)
                throw Error("FULLTEXT cannot be used with CREATE TABLE");
            return ParseCreateTableStatement();
        }
        else if (Check(TokenType.DATABASE))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE DATABASE");
            if (isFulltext)
                throw Error("FULLTEXT cannot be used with CREATE DATABASE");
            return ParseCreateDatabaseStatement();
        }
        else if (Check(TokenType.INDEX))
        {
            return ParseCreateIndexStatement(isUnique, isFulltext);
        }
        else if (Check(TokenType.VIEW))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE VIEW");
            return ParseCreateViewStatement();
        }
        else if (Check(TokenType.OR))
        {
            // CREATE OR REPLACE can be followed by VIEW, PROCEDURE, FUNCTION, TRIGGER, or EVENT
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE OR REPLACE");
            
            Advance(); // OR
            Expect(TokenType.REPLACE);
            
            // Now check what type of object to create and set OrReplace flag
            if (Check(TokenType.VIEW))
            {
                var stmt = ParseCreateViewStatement();
                if (stmt is CreateViewStatement viewStmt)
                    viewStmt.OrReplace = true;
                return stmt;
            }
            else if (Check(TokenType.PROCEDURE))
            {
                var stmt = (CreateProcedureStatement)ParseCreateProcedureStatement();
                stmt.OrReplace = true;
                return stmt;
            }
            else if (Check(TokenType.FUNCTION))
            {
                var stmt = (CreateFunctionStatement)ParseCreateFunctionStatement();
                stmt.OrReplace = true;
                return stmt;
            }
            else if (Check(TokenType.TRIGGER))
            {
                var stmt = (CreateTriggerStatement)ParseCreateTriggerStatement();
                stmt.OrReplace = true;
                return stmt;
            }
            else if (MatchIdentifier("EVENT") || Check(TokenType.EVENT))
            {
                var stmt = (CreateEventStatement)ParseCreateEventStatement();
                stmt.OrReplace = true;
                return stmt;
            }
            else
            {
                throw Error($"Unexpected token after CREATE OR REPLACE: {_currentToken.Value}");
            }
        }
        else if (Check(TokenType.USER))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE USER");
            return ParseCreateUserStatement();
        }
        else if (Check(TokenType.PROCEDURE))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE PROCEDURE");
            return ParseCreateProcedureStatement();
        }
        else if (Check(TokenType.FUNCTION))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE FUNCTION");
            return ParseCreateFunctionStatement();
        }
        else if (Check(TokenType.TRIGGER))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE TRIGGER");
            return ParseCreateTriggerStatement();
        }
        else if (Check(TokenType.EVENT))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE EVENT");
            return ParseCreateEventStatement();
        }

        throw Error($"Expected TABLE, DATABASE, INDEX, VIEW, USER, PROCEDURE, FUNCTION, TRIGGER, or EVENT after CREATE, got: {_currentToken.Value}");
    }

    private CreateUserStatement ParseCreateUserStatement()
    {
        Expect(TokenType.USER);

        var stmt = new CreateUserStatement();

        // Check for IF NOT EXISTS
        if (Match(TokenType.IF))
        {
            Expect(TokenType.NOT);
            Expect(TokenType.EXISTS);
            stmt.IfNotExists = true;
        }

        // Parse username@host
        stmt.UserName = ExpectIdentifier();
        if (Match(TokenType.AtAt) || (_currentToken.Value == "@" && Check(TokenType.Identifier)))
        {
            if (!Check(TokenType.AtAt))
                Advance(); // Skip @
            stmt.Host = ExpectIdentifier();
        }

        // Parse IDENTIFIED BY 'password'
        if (MatchIdentifier("IDENTIFIED"))
        {
            Expect(TokenType.BY);
            if (Check(TokenType.StringLiteral))
            {
                stmt.Password = _currentToken.Value;
                Advance();
            }
        }

        return stmt;
    }

    private CreateIndexStatement ParseCreateIndexStatement(bool isUnique, bool isFulltext = false)
    {
        Expect(TokenType.INDEX);

        var stmt = new CreateIndexStatement();
        stmt.IsUnique = isUnique;
        
        // Set index type based on FULLTEXT keyword
        if (isFulltext)
        {
            stmt.IndexType = IndexTypeAst.Fulltext;
        }

        // IF NOT EXISTS
        if (Match(TokenType.IF))
        {
            Expect(TokenType.NOT);
            Expect(TokenType.EXISTS);
            stmt.IfNotExists = true;
        }

        stmt.IndexName = ExpectIdentifier();

        Expect(TokenType.ON);

        stmt.TableName = ExpectIdentifier();
        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        // Column list
        Expect(TokenType.LeftParen);
        stmt.Columns = ParseIdentifierList();
        Expect(TokenType.RightParen);

        // Optional USING clause for index type (only if not already set to FULLTEXT)
        if (Match(TokenType.USING))
        {
            if (isFulltext)
            {
                throw Error("USING clause cannot be used with FULLTEXT index");
            }
            if (Match(TokenType.BTREE))
            {
                stmt.IndexType = IndexTypeAst.BTree;
            }
            else if (Match(TokenType.HASH))
            {
                stmt.IndexType = IndexTypeAst.Hash;
            }
            else
            {
                throw Error($"Expected BTREE or HASH, got: {_currentToken.Value}");
            }
        }

        return stmt;
    }

    private Statement ParseCreateViewStatement()
    {
        // Handle OR REPLACE
        bool orReplace = false;
        if (Match(TokenType.OR))
        {
            Expect(TokenType.REPLACE);
            orReplace = true;
        }

        Expect(TokenType.VIEW);

        var stmt = new CreateViewStatement();
        stmt.OrReplace = orReplace;

        // IF NOT EXISTS
        if (Match(TokenType.IF))
        {
            Expect(TokenType.NOT);
            Expect(TokenType.EXISTS);
            stmt.IfNotExists = true;
        }

        stmt.ViewName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.ViewName;
            stmt.ViewName = ExpectIdentifier();
        }

        // Optional column names
        if (Check(TokenType.LeftParen))
        {
            Advance();
            stmt.ColumnNames = ParseIdentifierList();
            Expect(TokenType.RightParen);
        }

        Expect(TokenType.AS);

        // Parse the SELECT statement
        stmt.Query = ParseSelectStatement();

        return stmt;
    }

    private CreateTableStatement ParseCreateTableStatement()
    {
        Expect(TokenType.TABLE);

        var stmt = new CreateTableStatement();

        // IF NOT EXISTS
        if (Match(TokenType.IF))
        {
            Expect(TokenType.NOT);
            Expect(TokenType.EXISTS);
            stmt.IfNotExists = true;
        }

        stmt.TableName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        Expect(TokenType.LeftParen);

        // Column definitions and constraints
        do
        {
            if (Check(TokenType.PRIMARY) || Check(TokenType.FOREIGN) || 
                Check(TokenType.UNIQUE) || Check(TokenType.CONSTRAINT) || Check(TokenType.CHECK))
            {
                stmt.Constraints.Add(ParseTableConstraint());
            }
            else
            {
                stmt.Columns.Add(ParseColumnDefinition());
            }
        } while (Match(TokenType.Comma));

        Expect(TokenType.RightParen);

        return stmt;
    }

    private ColumnDef ParseColumnDefinition()
    {
        var col = new ColumnDef();
        col.Name = ExpectIdentifier();
        col.DataType = ParseDataType(out var length, out var precision, out var scale, 
            out var enumValues, out var setValues);
        col.Length = length;
        col.Precision = precision;
        col.Scale = scale;
        col.EnumValues = enumValues;
        col.SetValues = setValues;

        // Column constraints
        while (true)
        {
            if (Match(TokenType.NOT))
            {
                Expect(TokenType.NULL);
                col.IsNullable = false;
            }
            else if (Match(TokenType.NULL))
            {
                col.IsNullable = true;
            }
            else if (Match(TokenType.PRIMARY))
            {
                Expect(TokenType.KEY);
                col.IsPrimaryKey = true;
                col.IsNullable = false;
            }
            else if (Match(TokenType.AUTO_INCREMENT))
            {
                col.IsAutoIncrement = true;
            }
            else if (Match(TokenType.UNIQUE))
            {
                col.IsUnique = true;
            }
            else if (Match(TokenType.DEFAULT))
            {
                col.DefaultValue = ParseExpression();
            }
            else
            {
                break;
            }
        }

        return col;
    }

    private DataType ParseDataType(out int? length, out int? precision, out int? scale)
    {
        return ParseDataType(out length, out precision, out scale, out _, out _);
    }

    private DataType ParseDataType(out int? length, out int? precision, out int? scale, 
        out List<string>? enumValues, out List<string>? setValues)
    {
        length = null;
        precision = null;
        scale = null;
        enumValues = null;
        setValues = null;

        DataType dataType;

        var token = _currentToken;
        Advance();

        dataType = token.Type switch
        {
            TokenType.INT or TokenType.INTEGER => DataType.Int,
            TokenType.BIGINT => DataType.BigInt,
            TokenType.SMALLINT => DataType.SmallInt,
            TokenType.TINYINT => DataType.TinyInt,
            TokenType.VARCHAR => DataType.VarChar,
            TokenType.CHAR => DataType.Char,
            TokenType.TEXT => DataType.Text,
            TokenType.BOOLEAN or TokenType.BOOL => DataType.Boolean,
            TokenType.DATETIME => DataType.DateTime,
            TokenType.DATE => DataType.Date,
            TokenType.TIME => DataType.Time,
            TokenType.TIMESTAMP => DataType.Timestamp,
            TokenType.FLOAT => DataType.Float,
            TokenType.DOUBLE => DataType.Double,
            TokenType.DECIMAL => DataType.Decimal,
            TokenType.BLOB => DataType.Blob,
            TokenType.JSON => DataType.Json,
            TokenType.GEOMETRY => DataType.Geometry,
            TokenType.ENUM => DataType.Enum,
            TokenType.SET => DataType.Set,
            _ => throw Error($"Expected data type, got: {token.Value}")
        };

        // Parse length/precision/scale or ENUM/SET values
        if (Match(TokenType.LeftParen))
        {
            if (dataType == DataType.Enum)
            {
                // Parse ENUM('a', 'b', 'c')
                enumValues = ParseEnumSetValues();
            }
            else if (dataType == DataType.Set)
            {
                // Parse SET('a', 'b', 'c')
                setValues = ParseEnumSetValues();
            }
            else
            {
                var first = ParseInteger();

                if (dataType == DataType.Decimal)
                {
                    precision = first;
                    if (Match(TokenType.Comma))
                    {
                        scale = ParseInteger();
                    }
                }
                else
                {
                    length = first;
                }
            }

            Expect(TokenType.RightParen);
        }

        return dataType;
    }

    /// <summary>
    /// Parses a list of string values for ENUM/SET types.
    /// </summary>
    private List<string> ParseEnumSetValues()
    {
        var values = new List<string>();
        
        do
        {
            var value = ExpectStringLiteral();
            values.Add(value);
        } while (Match(TokenType.Comma));

        if (values.Count == 0)
        {
            throw Error("ENUM/SET requires at least one value");
        }

        return values;
    }

    private TableConstraint ParseTableConstraint()
    {
        var constraint = new TableConstraint();

        // Optional constraint name
        if (Match(TokenType.CONSTRAINT))
        {
            constraint.Name = ExpectIdentifier();
        }

        if (Match(TokenType.PRIMARY))
        {
            Expect(TokenType.KEY);
            constraint.Type = ConstraintType.PrimaryKey;
            Expect(TokenType.LeftParen);
            constraint.Columns = ParseIdentifierList();
            Expect(TokenType.RightParen);
        }
        else if (Match(TokenType.UNIQUE))
        {
            constraint.Type = ConstraintType.Unique;
            Expect(TokenType.LeftParen);
            constraint.Columns = ParseIdentifierList();
            Expect(TokenType.RightParen);
        }
        else if (Match(TokenType.FOREIGN))
        {
            Expect(TokenType.KEY);
            constraint.Type = ConstraintType.ForeignKey;
            Expect(TokenType.LeftParen);
            constraint.Columns = ParseIdentifierList();
            Expect(TokenType.RightParen);
            Expect(TokenType.REFERENCES);
            constraint.ReferencedTable = ExpectIdentifier();
            Expect(TokenType.LeftParen);
            constraint.ReferencedColumns = ParseIdentifierList();
            Expect(TokenType.RightParen);

            // Parse optional ON DELETE/UPDATE actions
            var (onDelete, onUpdate) = ParseForeignKeyReferentialActions();
            constraint.OnDelete = onDelete;
            constraint.OnUpdate = onUpdate;
        }
        else if (Match(TokenType.CHECK))
        {
            constraint.Type = ConstraintType.Check;
            Expect(TokenType.LeftParen);
            constraint.CheckExpression = ParseExpression();
            Expect(TokenType.RightParen);
        }
        else
        {
            throw Error($"Expected constraint type, got: {_currentToken.Value}");
        }

        return constraint;
    }

    private CreateDatabaseStatement ParseCreateDatabaseStatement()
    {
        Expect(TokenType.DATABASE);

        var stmt = new CreateDatabaseStatement();

        if (Match(TokenType.IF))
        {
            Expect(TokenType.NOT);
            Expect(TokenType.EXISTS);
            stmt.IfNotExists = true;
        }

        stmt.DatabaseName = ExpectIdentifier();
        return stmt;
    }

    private Statement ParseDropStatement()
    {
        Expect(TokenType.DROP);

        if (Check(TokenType.TABLE))
        {
            Advance();
            var stmt = new DropTableStatement();

            if (Match(TokenType.IF))
            {
                Expect(TokenType.EXISTS);
                stmt.IfExists = true;
            }

            stmt.TableName = ExpectIdentifier();

            if (Match(TokenType.Dot))
            {
                stmt.DatabaseName = stmt.TableName;
                stmt.TableName = ExpectIdentifier();
            }

            return stmt;
        }
        else if (Check(TokenType.DATABASE))
        {
            Advance();
            var stmt = new DropDatabaseStatement();

            if (Match(TokenType.IF))
            {
                Expect(TokenType.EXISTS);
                stmt.IfExists = true;
            }

            stmt.DatabaseName = ExpectIdentifier();
            return stmt;
        }
        else if (Check(TokenType.INDEX))
        {
            return ParseDropIndexStatement();
        }
        else if (Check(TokenType.VIEW))
        {
            return ParseDropViewStatement();
        }
        else if (Check(TokenType.USER))
        {
            return ParseDropUserStatement();
        }
        else if (Check(TokenType.PROCEDURE))
        {
            return ParseDropProcedureStatement();
        }
        else if (Check(TokenType.FUNCTION))
        {
            return ParseDropFunctionStatement();
        }
        else if (Check(TokenType.TRIGGER))
        {
            return ParseDropTriggerStatement();
        }
        else if (Check(TokenType.EVENT))
        {
            return ParseDropEventStatement();
        }

        throw Error($"Expected TABLE, DATABASE, INDEX, VIEW, USER, PROCEDURE, FUNCTION, TRIGGER, or EVENT after DROP, got: {_currentToken.Value}");
    }

    private DropUserStatement ParseDropUserStatement()
    {
        Expect(TokenType.USER);

        var stmt = new DropUserStatement();

        if (Match(TokenType.IF))
        {
            Expect(TokenType.EXISTS);
            stmt.IfExists = true;
        }

        // Parse username@host
        stmt.UserName = ExpectIdentifier();
        if (Match(TokenType.AtAt) || (_currentToken.Value == "@" && Check(TokenType.Identifier)))
        {
            if (!Check(TokenType.AtAt))
                Advance(); // Skip @
            stmt.Host = ExpectIdentifier();
        }

        return stmt;
    }

    private AlterUserStatement ParseAlterUserStatement()
    {
        Expect(TokenType.USER);

        var stmt = new AlterUserStatement();

        // Parse username@host
        stmt.UserName = ExpectIdentifier();
        if (Match(TokenType.AtAt) || (_currentToken.Value == "@" && Check(TokenType.Identifier)))
        {
            if (!Check(TokenType.AtAt))
                Advance(); // Skip @
            stmt.Host = ExpectIdentifier();
        }

        // Parse IDENTIFIED BY 'password'
        if (MatchIdentifier("IDENTIFIED"))
        {
            Expect(TokenType.BY);
            if (Check(TokenType.StringLiteral))
            {
                stmt.Password = _currentToken.Value;
                Advance();
            }
        }

        return stmt;
    }

    private DropIndexStatement ParseDropIndexStatement()
    {
        Expect(TokenType.INDEX);

        var stmt = new DropIndexStatement();

        if (Match(TokenType.IF))
        {
            Expect(TokenType.EXISTS);
            stmt.IfExists = true;
        }

        stmt.IndexName = ExpectIdentifier();

        Expect(TokenType.ON);

        stmt.TableName = ExpectIdentifier();
        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        return stmt;
    }

    private DropViewStatement ParseDropViewStatement()
    {
        Expect(TokenType.VIEW);

        var stmt = new DropViewStatement();

        if (Match(TokenType.IF))
        {
            Expect(TokenType.EXISTS);
            stmt.IfExists = true;
        }

        stmt.ViewName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.ViewName;
            stmt.ViewName = ExpectIdentifier();
        }

        return stmt;
    }

    /// <summary>
    /// Parses an ALTER statement (TABLE or USER).
    /// </summary>
    private Statement ParseAlterStatement()
    {
        Expect(TokenType.ALTER);

        if (Check(TokenType.USER))
        {
            return ParseAlterUserStatement();
        }

        Expect(TokenType.TABLE);

        var stmt = new AlterTableStatement();
        stmt.TableName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        // Parse one or more actions
        do
        {
            stmt.Actions.Add(ParseAlterTableAction());
        }
        while (Match(TokenType.Comma));

        // Parse optional ALGORITHM clause
        if (Match(TokenType.Comma))
        {
            // Allow comma before ALGORITHM/LOCK
        }
        
        if (Match(TokenType.ALGORITHM))
        {
            Expect(TokenType.Equal);
            stmt.Algorithm = ParseAlterAlgorithm();
        }

        // Parse optional LOCK clause
        if (Match(TokenType.Comma))
        {
            // Allow comma before LOCK
        }
        
        if (Match(TokenType.LOCK))
        {
            Expect(TokenType.Equal);
            stmt.Lock = ParseAlterLockMode();
        }

        return stmt;
    }

    /// <summary>
    /// Parses ALGORITHM=INPLACE/COPY/DEFAULT value.
    /// </summary>
    private AlterAlgorithm ParseAlterAlgorithm()
    {
        if (Match(TokenType.INPLACE))
            return AlterAlgorithm.Inplace;
        if (Match(TokenType.COPY))
            return AlterAlgorithm.Copy;
        if (Match(TokenType.DEFAULT))
            return AlterAlgorithm.Default;
        
        // Also allow as identifiers for compatibility
        var identifier = ExpectIdentifierOrKeyword();
        return identifier.ToUpperInvariant() switch
        {
            "INPLACE" => AlterAlgorithm.Inplace,
            "COPY" => AlterAlgorithm.Copy,
            "DEFAULT" => AlterAlgorithm.Default,
            "INSTANT" => AlterAlgorithm.Inplace, // Treat INSTANT as INPLACE
            _ => throw Error($"Unknown ALGORITHM value: {identifier}")
        };
    }

    /// <summary>
    /// Parses LOCK=NONE/SHARED/EXCLUSIVE/DEFAULT value.
    /// </summary>
    private AlterLockMode ParseAlterLockMode()
    {
        if (Match(TokenType.NONE))
            return AlterLockMode.None;
        if (Match(TokenType.SHARED))
            return AlterLockMode.Shared;
        if (Match(TokenType.EXCLUSIVE))
            return AlterLockMode.Exclusive;
        if (Match(TokenType.DEFAULT))
            return AlterLockMode.Default;
        
        // Also allow as identifiers for compatibility
        var identifier = ExpectIdentifierOrKeyword();
        return identifier.ToUpperInvariant() switch
        {
            "NONE" => AlterLockMode.None,
            "SHARED" => AlterLockMode.Shared,
            "EXCLUSIVE" => AlterLockMode.Exclusive,
            "DEFAULT" => AlterLockMode.Default,
            _ => throw Error($"Unknown LOCK value: {identifier}")
        };
    }

    /// <summary>
    /// Parses a single ALTER TABLE action.
    /// </summary>
    private AlterTableAction ParseAlterTableAction()
    {
        if (Match(TokenType.ADD))
        {
            return ParseAddAction();
        }
        else if (Match(TokenType.DROP))
        {
            return ParseDropAction();
        }
        else if (Match(TokenType.MODIFY))
        {
            return ParseModifyColumnAction();
        }
        else if (Match(TokenType.CHANGE))
        {
            return ParseChangeColumnAction();
        }
        else if (Match(TokenType.RENAME))
        {
            return ParseRenameAction();
        }

        throw Error($"Expected ADD, DROP, MODIFY, CHANGE, or RENAME in ALTER TABLE, got: {_currentToken.Value}");
    }

    private AlterTableAction ParseAddAction()
    {
        // ADD COLUMN, ADD INDEX, ADD PRIMARY KEY, ADD FOREIGN KEY, ADD CONSTRAINT, ADD UNIQUE
        if (Match(TokenType.COLUMN))
        {
            return ParseAddColumnAction();
        }
        else if (Match(TokenType.INDEX) || Match(TokenType.KEY))
        {
            return ParseAddIndexAction();
        }
        else if (Check(TokenType.PRIMARY))
        {
            Advance();
            Expect(TokenType.KEY);
            return ParseAddPrimaryKeyAction();
        }
        else if (Check(TokenType.FOREIGN))
        {
            Advance();
            Expect(TokenType.KEY);
            return ParseAddForeignKeyAction();
        }
        else if (Match(TokenType.UNIQUE))
        {
            // ADD UNIQUE [INDEX/KEY] index_name (columns)
            Match(TokenType.INDEX);
            Match(TokenType.KEY);
            return ParseAddIndexAction(isUnique: true);
        }
        else if (Match(TokenType.CONSTRAINT))
        {
            return ParseAddConstraintAction();
        }
        else
        {
            // Default to ADD COLUMN
            return ParseAddColumnAction();
        }
    }

    private AddColumnAction ParseAddColumnAction()
    {
        var action = new AddColumnAction();
        action.Column = ParseColumnDefinition();

        // Check for FIRST or AFTER
        if (Match(TokenType.FIRST))
        {
            action.IsFirst = true;
        }
        else if (Match(TokenType.AFTER))
        {
            action.AfterColumn = ExpectIdentifier();
        }

        return action;
    }

    private AddIndexAction ParseAddIndexAction(bool isUnique = false)
    {
        var action = new AddIndexAction();
        action.IsUnique = isUnique;

        // Optional index name
        if (Check(TokenType.Identifier))
        {
            action.IndexName = ExpectIdentifier();
        }

        Expect(TokenType.LeftParen);
        action.Columns = ParseIdentifierList();
        Expect(TokenType.RightParen);

        return action;
    }

    private AddPrimaryKeyAction ParseAddPrimaryKeyAction()
    {
        var action = new AddPrimaryKeyAction();

        Expect(TokenType.LeftParen);
        action.Columns = ParseIdentifierList();
        Expect(TokenType.RightParen);

        return action;
    }

    private AddForeignKeyAction ParseAddForeignKeyAction()
    {
        var action = new AddForeignKeyAction();

        // Optional constraint name  
        if (Check(TokenType.Identifier))
        {
            action.ConstraintName = ExpectIdentifier();
        }

        Expect(TokenType.LeftParen);
        action.Columns = ParseIdentifierList();
        Expect(TokenType.RightParen);

        Expect(TokenType.REFERENCES);
        action.ReferencedTable = ExpectIdentifier();

        Expect(TokenType.LeftParen);
        action.ReferencedColumns = ParseIdentifierList();
        Expect(TokenType.RightParen);

        // Parse optional ON DELETE/UPDATE actions
        var (onDelete, onUpdate) = ParseForeignKeyReferentialActions();
        action.OnDelete = onDelete;
        action.OnUpdate = onUpdate;

        return action;
    }

    /// <summary>
    /// Parses optional ON DELETE and ON UPDATE referential actions.
    /// Syntax: [ON DELETE action] [ON UPDATE action]
    /// Actions: RESTRICT | CASCADE | SET NULL | SET DEFAULT | NO ACTION
    /// </summary>
    private (ForeignKeyReferentialAction OnDelete, ForeignKeyReferentialAction OnUpdate) ParseForeignKeyReferentialActions()
    {
        var onDelete = ForeignKeyReferentialAction.Restrict;
        var onUpdate = ForeignKeyReferentialAction.Restrict;

        // ON DELETE and ON UPDATE can appear in any order
        for (int i = 0; i < 2; i++)
        {
            if (!Check(TokenType.ON))
                break;

            Advance(); // consume ON

            if (Match(TokenType.DELETE))
            {
                onDelete = ParseForeignKeyAction();
            }
            else if (Match(TokenType.UPDATE))
            {
                onUpdate = ParseForeignKeyAction();
            }
            else
            {
                throw Error($"Expected DELETE or UPDATE after ON, got: {_currentToken.Value}");
            }
        }

        return (onDelete, onUpdate);
    }

    /// <summary>
    /// Parses a single referential action.
    /// Actions: RESTRICT | CASCADE | SET NULL | SET DEFAULT | NO ACTION
    /// </summary>
    private ForeignKeyReferentialAction ParseForeignKeyAction()
    {
        if (Match(TokenType.RESTRICT))
        {
            return ForeignKeyReferentialAction.Restrict;
        }
        else if (Match(TokenType.CASCADE))
        {
            return ForeignKeyReferentialAction.Cascade;
        }
        else if (Match(TokenType.SET))
        {
            if (Match(TokenType.NULL))
            {
                return ForeignKeyReferentialAction.SetNull;
            }
            else if (Match(TokenType.DEFAULT))
            {
                return ForeignKeyReferentialAction.SetDefault;
            }
            else
            {
                throw Error($"Expected NULL or DEFAULT after SET, got: {_currentToken.Value}");
            }
        }
        else if (Match(TokenType.NO))
        {
            Expect(TokenType.ACTION);
            return ForeignKeyReferentialAction.NoAction;
        }
        else
        {
            throw Error($"Expected referential action (RESTRICT, CASCADE, SET NULL, SET DEFAULT, NO ACTION), got: {_currentToken.Value}");
        }
    }

    private AddConstraintAction ParseAddConstraintAction()
    {
        var action = new AddConstraintAction();
        var constraint = new TableConstraint();

        // Constraint name
        constraint.Name = ExpectIdentifier();

        // Constraint type
        if (Check(TokenType.PRIMARY))
        {
            Advance();
            Expect(TokenType.KEY);
            constraint.Type = ConstraintType.PrimaryKey;

            Expect(TokenType.LeftParen);
            constraint.Columns = ParseIdentifierList();
            Expect(TokenType.RightParen);
        }
        else if (Match(TokenType.UNIQUE))
        {
            constraint.Type = ConstraintType.Unique;

            Expect(TokenType.LeftParen);
            constraint.Columns = ParseIdentifierList();
            Expect(TokenType.RightParen);
        }
        else if (Check(TokenType.FOREIGN))
        {
            Advance();
            Expect(TokenType.KEY);
            constraint.Type = ConstraintType.ForeignKey;

            Expect(TokenType.LeftParen);
            constraint.Columns = ParseIdentifierList();
            Expect(TokenType.RightParen);

            Expect(TokenType.REFERENCES);
            constraint.ReferencedTable = ExpectIdentifier();

            Expect(TokenType.LeftParen);
            constraint.ReferencedColumns = ParseIdentifierList();
            Expect(TokenType.RightParen);

            // Parse optional ON DELETE/UPDATE actions
            var (onDelete, onUpdate) = ParseForeignKeyReferentialActions();
            constraint.OnDelete = onDelete;
            constraint.OnUpdate = onUpdate;
        }
        else if (Match(TokenType.CHECK))
        {
            constraint.Type = ConstraintType.Check;
            Expect(TokenType.LeftParen);
            constraint.CheckExpression = ParseExpression();
            Expect(TokenType.RightParen);
        }
        else
        {
            throw Error($"Expected PRIMARY KEY, UNIQUE, FOREIGN KEY, or CHECK after constraint name");
        }

        action.Constraint = constraint;
        return action;
    }

    private AlterTableAction ParseDropAction()
    {
        if (Match(TokenType.COLUMN))
        {
            var name = ExpectIdentifier();
            return new DropColumnAction { ColumnName = name };
        }
        else if (Match(TokenType.INDEX) || Match(TokenType.KEY))
        {
            var name = ExpectIdentifier();
            return new DropIndexAction { IndexName = name };
        }
        else if (Check(TokenType.PRIMARY))
        {
            Advance();
            Expect(TokenType.KEY);
            return new DropPrimaryKeyAction();
        }
        else if (Check(TokenType.FOREIGN))
        {
            Advance();
            Expect(TokenType.KEY);
            var name = ExpectIdentifier();
            return new DropForeignKeyAction { ConstraintName = name };
        }
        else if (Match(TokenType.CONSTRAINT))
        {
            var name = ExpectIdentifier();
            return new DropConstraintAction { ConstraintName = name };
        }
        else
        {
            // Default to DROP COLUMN
            var name = ExpectIdentifier();
            return new DropColumnAction { ColumnName = name };
        }
    }

    private ModifyColumnAction ParseModifyColumnAction()
    {
        Match(TokenType.COLUMN); // Optional COLUMN keyword
        
        var action = new ModifyColumnAction();
        action.Column = ParseColumnDefinition();

        if (Match(TokenType.FIRST))
        {
            action.IsFirst = true;
        }
        else if (Match(TokenType.AFTER))
        {
            action.AfterColumn = ExpectIdentifier();
        }

        return action;
    }

    private ChangeColumnAction ParseChangeColumnAction()
    {
        Match(TokenType.COLUMN); // Optional COLUMN keyword

        var action = new ChangeColumnAction();
        action.OldColumnName = ExpectIdentifier();
        action.NewColumn = ParseColumnDefinition();

        if (Match(TokenType.FIRST))
        {
            action.IsFirst = true;
        }
        else if (Match(TokenType.AFTER))
        {
            action.AfterColumn = ExpectIdentifier();
        }

        return action;
    }

    private AlterTableAction ParseRenameAction()
    {
        if (Match(TokenType.COLUMN))
        {
            var oldName = ExpectIdentifier();
            Expect(TokenType.TO);
            var newName = ExpectIdentifier();
            return new RenameColumnAction { OldName = oldName, NewName = newName };
        }
        else if (Match(TokenType.TO) || Match(TokenType.AS))
        {
            var newName = ExpectIdentifier();
            return new RenameTableAction { NewName = newName };
        }
        else
        {
            // RENAME old_name TO new_name (column rename without COLUMN keyword)
            var oldName = ExpectIdentifier();
            Expect(TokenType.TO);
            var newName = ExpectIdentifier();
            return new RenameColumnAction { OldName = oldName, NewName = newName };
        }
    }

    #endregion

    #region Utility Statements

    private UseDatabaseStatement ParseUseStatement()
    {
        Expect(TokenType.USE);
        return new UseDatabaseStatement { DatabaseName = ExpectIdentifier() };
    }

    private Statement ParseShowStatement()
    {
        Expect(TokenType.SHOW);

        // Check for GLOBAL or SESSION scope
        var scope = SetScope.Session;
        if (Match(TokenType.GLOBAL))
        {
            scope = SetScope.Global;
        }
        else if (Match(TokenType.SESSION))
        {
            scope = SetScope.Session;
        }

        // Handle SHOW FULL TABLES, SHOW FULL COLUMNS, etc. (check FULL before TABLES)
        if (Match(TokenType.FULL))
        {
            if (Check(TokenType.TABLES))
            {
                Advance();
                var stmt = new ShowTablesStatement { IsFull = true };
                if (Match(TokenType.FROM) || Match(TokenType.IN))
                {
                    stmt.DatabaseName = ExpectIdentifier();
                }
                // Parse optional LIKE pattern
                if (Match(TokenType.LIKE))
                {
                    stmt.LikePattern = ExpectString();
                }
                // Parse optional WHERE clause
                else if (Match(TokenType.WHERE))
                {
                    stmt.Where = ParseExpression();
                }
                return stmt;
            }
            else if (Check(TokenType.COLUMNS) || MatchIdentifier("FIELDS"))
            {
                if (Check(TokenType.COLUMNS)) Advance();
                Expect(TokenType.FROM);
                var stmt = new ShowColumnsStatement();
                stmt.TableName = ExpectIdentifier();
                if (Match(TokenType.Dot))
                {
                    stmt.DatabaseName = stmt.TableName;
                    stmt.TableName = ExpectIdentifier();
                }
                if (Match(TokenType.FROM))
                {
                    stmt.DatabaseName = ExpectIdentifier();
                }
                return stmt;
            }
            else
            {
                throw Error($"Expected TABLES or COLUMNS after SHOW FULL, got: {_currentToken.Value}");
            }
        }

        if (Check(TokenType.TABLES))
        {
            Advance();
            var stmt = new ShowTablesStatement();
            if (Match(TokenType.FROM) || Match(TokenType.IN))
            {
                stmt.DatabaseName = ExpectIdentifier();
            }
            // Parse optional LIKE pattern
            if (Match(TokenType.LIKE))
            {
                stmt.LikePattern = ExpectString();
            }
            // Parse optional WHERE clause
            else if (Match(TokenType.WHERE))
            {
                stmt.Where = ParseExpression();
            }
            return stmt;
        }
        else if (Check(TokenType.DATABASES))
        {
            Advance();
            return new ShowDatabasesStatement();
        }
        else if (Check(TokenType.VARIABLES))
        {
            Advance();
            var stmt = new ShowVariablesStatement { Scope = scope };
            if (Match(TokenType.LIKE))
            {
                stmt.LikePattern = ExpectString();
            }
            return stmt;
        }
        else if (Check(TokenType.STATUS))
        {
            Advance();
            var stmt = new ShowStatusStatement { Scope = scope };
            if (Match(TokenType.LIKE))
            {
                stmt.LikePattern = ExpectString();
            }
            return stmt;
        }
        else if (Check(TokenType.CREATE))
        {
            Advance();
            if (Match(TokenType.TABLE))
            {
                var stmt = new ShowCreateTableStatement();
                stmt.TableName = ExpectIdentifier();
                if (Match(TokenType.Dot))
                {
                    stmt.DatabaseName = stmt.TableName;
                    stmt.TableName = ExpectIdentifier();
                }
                return stmt;
            }
            else if (Match(TokenType.DATABASE))
            {
                // SHOW CREATE DATABASE - return as ShowDatabasesStatement for compatibility
                ExpectIdentifier(); // Consume database name
                return new ShowDatabasesStatement();
            }
            throw Error($"Expected TABLE or DATABASE after SHOW CREATE, got: {_currentToken.Value}");
        }
        else if (Check(TokenType.COLUMNS) || MatchIdentifier("FIELDS"))
        {
            if (Check(TokenType.COLUMNS)) Advance();
            Expect(TokenType.FROM);
            var stmt = new ShowColumnsStatement();
            stmt.TableName = ExpectIdentifier();
            if (Match(TokenType.Dot))
            {
                stmt.DatabaseName = stmt.TableName;
                stmt.TableName = ExpectIdentifier();
            }
            if (Match(TokenType.FROM))
            {
                stmt.DatabaseName = ExpectIdentifier();
            }
            if (Match(TokenType.LIKE))
            {
                stmt.LikePattern = ExpectString();
            }
            return stmt;
        }
        else if (Check(TokenType.INDEX) || MatchIdentifier("INDEXES") || MatchIdentifier("KEYS"))
        {
            if (Check(TokenType.INDEX)) Advance();
            Expect(TokenType.FROM);
            var stmt = new ShowIndexStatement();
            stmt.TableName = ExpectIdentifier();
            if (Match(TokenType.Dot))
            {
                stmt.DatabaseName = stmt.TableName;
                stmt.TableName = ExpectIdentifier();
            }
            if (Match(TokenType.FROM))
            {
                stmt.DatabaseName = ExpectIdentifier();
            }
            return stmt;
        }
        else if (Check(TokenType.WARNINGS))
        {
            Advance();
            return new ShowWarningsStatement();
        }
        else if (Check(TokenType.ERRORS))
        {
            Advance();
            return new ShowErrorsStatement();
        }
        else if (Check(TokenType.COLLATION))
        {
            Advance();
            var stmt = new ShowCollationStatement();
            if (Match(TokenType.LIKE))
            {
                stmt.LikePattern = ExpectString();
            }
            return stmt;
        }
        else if (Check(TokenType.CHARSET) || MatchIdentifier("CHARACTER"))
        {
            if (Check(TokenType.CHARSET))
            {
                Advance();
            }
            else
            {
                // CHARACTER SET
                Expect(TokenType.SET);
            }
            var stmt = new ShowCharsetStatement();
            if (Match(TokenType.LIKE))
            {
                stmt.LikePattern = ExpectString();
            }
            return stmt;
        }
        // SHOW PROCESSLIST - return empty for compatibility
        else if (MatchIdentifier("PROCESSLIST"))
        {
            return new ShowStatusStatement();
        }
        // SHOW ENGINE - return empty for compatibility
        else if (MatchIdentifier("ENGINE") || MatchIdentifier("ENGINES"))
        {
            // Skip remaining tokens until end of statement
            while (!Check(TokenType.Semicolon) && !Check(TokenType.EOF))
            {
                Advance();
            }
            return new ShowStatusStatement();
        }
        // SHOW TABLE STATUS - returns table metadata
        else if (Check(TokenType.TABLE))
        {
            Advance();
            if (Match(TokenType.STATUS))
            {
                var stmt = new ShowTableStatusStatement();
                if (Match(TokenType.FROM) || Match(TokenType.IN))
                {
                    stmt.DatabaseName = ExpectIdentifier();
                }
                if (Match(TokenType.LIKE))
                {
                    stmt.LikePattern = ExpectString();
                }
                else if (Match(TokenType.WHERE))
                {
                    stmt.Where = ParseExpression();
                }
                return stmt;
            }
            throw Error($"Expected STATUS after TABLE, got: {_currentToken.Value}");
        }

        throw Error($"Unexpected token after SHOW: {_currentToken.Value}");
    }

    private string ExpectString()
    {
        if (!Check(TokenType.StringLiteral))
        {
            throw Error($"Expected string literal, got: {_currentToken.Value}");
        }
        var value = _currentToken.Value;
        Advance();
        return value;
    }

    private DescribeStatement ParseDescribeStatement()
    {
        Expect(TokenType.DESCRIBE);
        var stmt = new DescribeStatement();
        stmt.TableName = ExpectIdentifier();

        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = stmt.TableName;
            stmt.TableName = ExpectIdentifier();
        }

        return stmt;
    }

    #endregion

    #region Transaction Statements

    private BeginStatement ParseBeginStatement()
    {
        Expect(TokenType.BEGIN);
        return new BeginStatement();
    }

    private CommitStatement ParseCommitStatement()
    {
        Expect(TokenType.COMMIT);
        return new CommitStatement();
    }

    private RollbackStatement ParseRollbackStatement()
    {
        Expect(TokenType.ROLLBACK);
        return new RollbackStatement();
    }

    private BeginStatement ParseStartStatement()
    {
        Expect(TokenType.START);
        Expect(TokenType.TRANSACTION);
        return new BeginStatement();
    }

    #endregion

    #region SET Statement

    private Statement ParseSetStatement()
    {
        Expect(TokenType.SET);

        // Check for SET [GLOBAL|SESSION] TRANSACTION
        var scope = SetScope.Session;
        if (Check(TokenType.GLOBAL))
        {
            Advance();
            scope = SetScope.Global;
        }
        else if (Check(TokenType.SESSION))
        {
            Advance();
            scope = SetScope.Session;
        }

        if (Check(TokenType.TRANSACTION))
        {
            return ParseSetTransactionStatement(scope);
        }

        // If we consumed GLOBAL or SESSION but it's not TRANSACTION, 
        // we need to handle it as a variable setting

        var stmt = new SetStatement();

        // Check for SET NAMES
        if (Check(TokenType.NAMES))
        {
            Advance();
            stmt.IsSetNames = true;
            stmt.Charset = ExpectIdentifierOrKeyword();
            
            // Optional COLLATE clause
            if (Match(TokenType.COLLATION) || MatchIdentifier("COLLATE"))
            {
                stmt.Collation = ExpectIdentifierOrKeyword();
            }
            return stmt;
        }

        // Check for SET CHARSET
        if (Check(TokenType.CHARSET))
        {
            Advance();
            stmt.IsSetNames = true;
            stmt.Charset = ExpectIdentifierOrKeyword();
            return stmt;
        }

        // Parse variable assignments: SET var = value [, var = value ...]
        do
        {
            var setVar = new SetVariable();
            setVar.Scope = scope;

            // Check for GLOBAL or SESSION scope (if not already set)
            if (scope == SetScope.Session)
            {
                if (Match(TokenType.GLOBAL))
                {
                    setVar.Scope = SetScope.Global;
                }
                else if (Match(TokenType.SESSION))
                {
                    setVar.Scope = SetScope.Session;
                }
            }

            // Check for @@global. or @@session. prefix
            if (Check(TokenType.AtAt))
            {
                Advance();
                var varName = ExpectIdentifierOrKeyword();
                
                if (varName.Equals("global", StringComparison.OrdinalIgnoreCase) && Match(TokenType.Dot))
                {
                    setVar.Scope = SetScope.Global;
                    setVar.Name = ExpectIdentifierOrKeyword();
                }
                else if (varName.Equals("session", StringComparison.OrdinalIgnoreCase) && Match(TokenType.Dot))
                {
                    setVar.Scope = SetScope.Session;
                    setVar.Name = ExpectIdentifierOrKeyword();
                }
                else
                {
                    setVar.Name = varName;
                }
            }
            else
            {
                setVar.Name = ExpectIdentifierOrKeyword();
            }

            Expect(TokenType.Equal);
            setVar.Value = ParseExpression();
            stmt.Variables.Add(setVar);
        } while (Match(TokenType.Comma));

        return stmt;
    }

    private SetTransactionStatement ParseSetTransactionStatement(SetScope scope)
    {
        Expect(TokenType.TRANSACTION);
        var stmt = new SetTransactionStatement { Scope = scope };

        // Parse characteristics
        while (true)
        {
            if (Check(TokenType.ISOLATION))
            {
                Advance();
                Expect(TokenType.LEVEL);
                stmt.IsolationLevel = ParseIsolationLevel();
            }
            else if (Check(TokenType.READ))
            {
                Advance();
                if (Check(TokenType.WRITE))
                {
                    Advance();
                    stmt.AccessMode = TransactionAccessMode.ReadWrite;
                }
                else if (MatchIdentifier("ONLY"))
                {
                    stmt.AccessMode = TransactionAccessMode.ReadOnly;
                }
                else
                {
                    throw Error("Expected WRITE or ONLY after READ");
                }
            }
            else
            {
                break;
            }

            // Check for comma to continue parsing more characteristics
            if (!Match(TokenType.Comma))
            {
                break;
            }
        }

        return stmt;
    }

    private TransactionIsolationLevel ParseIsolationLevel()
    {
        if (Check(TokenType.READ))
        {
            Advance();
            if (Check(TokenType.UNCOMMITTED))
            {
                Advance();
                return TransactionIsolationLevel.ReadUncommitted;
            }
            else if (Check(TokenType.COMMITTED))
            {
                Advance();
                return TransactionIsolationLevel.ReadCommitted;
            }
            else
            {
                throw Error("Expected UNCOMMITTED or COMMITTED after READ");
            }
        }
        else if (Check(TokenType.REPEATABLE))
        {
            Advance();
            Expect(TokenType.READ);
            return TransactionIsolationLevel.RepeatableRead;
        }
        else if (Check(TokenType.SERIALIZABLE))
        {
            Advance();
            return TransactionIsolationLevel.Serializable;
        }
        else
        {
            throw Error("Expected isolation level (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, or SERIALIZABLE)");
        }
    }

    private Statement ParseKillStatement()
    {
        Expect(TokenType.KILL);
        // Skip the connection ID - we don't actually support killing connections
        if (Check(TokenType.IntegerLiteral))
        {
            Advance();
        }
        // Return a no-op statement (just acknowledge the command)
        return new SetStatement { Variables = [] };
    }

    /// <summary>
    /// Expects an identifier or treats a keyword as an identifier.
    /// Used for variable names which might be reserved words.
    /// </summary>
    private string ExpectIdentifierOrKeyword()
    {
        // Accept identifier
        if (Check(TokenType.Identifier))
        {
            var value = _currentToken.Value;
            Advance();
            return value;
        }

        // Accept keywords as identifiers (for variable names like 'sql_mode', 'autocommit', etc.)
        if (_currentToken.Type >= TokenType.SELECT)
        {
            var value = _currentToken.Value;
            Advance();
            return value;
        }

        throw Error($"Expected identifier, got: {_currentToken.Value}");
    }

    /// <summary>
    /// Matches an identifier with a specific value (case-insensitive).
    /// </summary>
    private bool MatchIdentifier(string value)
    {
        if (Check(TokenType.Identifier) && _currentToken.Value.Equals(value, StringComparison.OrdinalIgnoreCase))
        {
            Advance();
            return true;
        }
        return false;
    }

    /// <summary>
    /// Expects an identifier with a specific value (case-insensitive).
    /// Throws if the identifier doesn't match.
    /// </summary>
    private void ExpectIdentifierValue(string value)
    {
        if (!MatchIdentifier(value))
        {
            throw Error($"Expected '{value}', got: {_currentToken.Value}");
        }
    }

    /// <summary>
    /// Expects a string literal and returns its value.
    /// </summary>
    private string ExpectStringLiteral()
    {
        if (!Check(TokenType.StringLiteral))
        {
            throw Error($"Expected string literal, got: {_currentToken.Value}");
        }

        var value = _currentToken.Value;
        Advance();
        return value;
    }

    #endregion

    #region Expression Parsing

    private Expression ParseExpression()
    {
        return ParseOrExpression();
    }

    private Expression ParseOrExpression()
    {
        var left = ParseAndExpression();

        while (Match(TokenType.OR))
        {
            var right = ParseAndExpression();
            left = new BinaryExpression
            {
                Left = left,
                Operator = BinaryOperator.Or,
                Right = right
            };
        }

        return left;
    }

    private Expression ParseAndExpression()
    {
        var left = ParseNotExpression();

        while (Match(TokenType.AND))
        {
            var right = ParseNotExpression();
            left = new BinaryExpression
            {
                Left = left,
                Operator = BinaryOperator.And,
                Right = right
            };
        }

        return left;
    }

    private Expression ParseNotExpression()
    {
        if (Match(TokenType.NOT))
        {
            return new UnaryExpression
            {
                Operator = UnaryOperator.Not,
                Operand = ParseNotExpression()
            };
        }

        return ParseComparisonExpression();
    }

    private Expression ParseComparisonExpression()
    {
        var left = ParseAdditionExpression();

        // IS NULL / IS NOT NULL
        if (Check(TokenType.IS))
        {
            Advance();
            bool isNot = Match(TokenType.NOT);
            Expect(TokenType.NULL);
            return new IsNullExpression { Expression = left, IsNot = isNot };
        }

        // IN
        if (Check(TokenType.IN) || (Check(TokenType.NOT) && Peek().Type == TokenType.IN))
        {
            bool isNot = Match(TokenType.NOT);
            Expect(TokenType.IN);
            return ParseInExpression(left, isNot);
        }

        // BETWEEN
        if (Check(TokenType.BETWEEN) || (Check(TokenType.NOT) && Peek().Type == TokenType.BETWEEN))
        {
            bool isNot = Match(TokenType.NOT);
            Expect(TokenType.BETWEEN);
            var low = ParseAdditionExpression();
            Expect(TokenType.AND);
            var high = ParseAdditionExpression();
            return new BetweenExpression { Expression = left, Low = low, High = high, IsNot = isNot };
        }

        // LIKE
        if (Match(TokenType.LIKE))
        {
            var right = ParseAdditionExpression();
            return new BinaryExpression
            {
                Left = left,
                Operator = BinaryOperator.Like,
                Right = right
            };
        }

        // Comparison operators
        BinaryOperator? op = null;

        if (Match(TokenType.Equal)) op = BinaryOperator.Equal;
        else if (Match(TokenType.NotEqual)) op = BinaryOperator.NotEqual;
        else if (Match(TokenType.LessThan)) op = BinaryOperator.LessThan;
        else if (Match(TokenType.LessThanOrEqual)) op = BinaryOperator.LessThanOrEqual;
        else if (Match(TokenType.GreaterThan)) op = BinaryOperator.GreaterThan;
        else if (Match(TokenType.GreaterThanOrEqual)) op = BinaryOperator.GreaterThanOrEqual;

        if (op.HasValue)
        {
            // Check for quantified subquery (ALL/ANY/SOME)
            if (Check(TokenType.ALL) || Check(TokenType.ANY) || Check(TokenType.SOME))
            {
                QuantifierType quantifier;
                if (Match(TokenType.ALL))
                    quantifier = QuantifierType.All;
                else if (Match(TokenType.ANY))
                    quantifier = QuantifierType.Any;
                else // SOME
                {
                    Advance();
                    quantifier = QuantifierType.Some;
                }

                Expect(TokenType.LeftParen);
                var subquery = ParseSelectStatement();
                Expect(TokenType.RightParen);

                return new QuantifiedComparisonExpression
                {
                    Expression = left,
                    Operator = op.Value,
                    Quantifier = quantifier,
                    Subquery = subquery
                };
            }

            // Regular binary comparison
            var right = ParseAdditionExpression();
            return new BinaryExpression
            {
                Left = left,
                Operator = op.Value,
                Right = right
            };
        }

        return left;
    }

    private InExpression ParseInExpression(Expression expr, bool isNot)
    {
        Expect(TokenType.LeftParen);

        var inExpr = new InExpression { Expression = expr, IsNot = isNot };

        if (Check(TokenType.SELECT))
        {
            inExpr.Subquery = ParseSelectStatement();
        }
        else
        {
            inExpr.Values = ParseExpressionList();
        }

        Expect(TokenType.RightParen);
        return inExpr;
    }

    private Expression ParseAdditionExpression()
    {
        var left = ParseMultiplicationExpression();

        while (true)
        {
            BinaryOperator? op = null;

            if (Match(TokenType.Plus)) op = BinaryOperator.Add;
            else if (Match(TokenType.Minus)) op = BinaryOperator.Subtract;

            if (!op.HasValue) break;

            var right = ParseMultiplicationExpression();
            left = new BinaryExpression
            {
                Left = left,
                Operator = op.Value,
                Right = right
            };
        }

        return left;
    }

    private Expression ParseMultiplicationExpression()
    {
        var left = ParseUnaryExpression();

        while (true)
        {
            BinaryOperator? op = null;

            if (Match(TokenType.Asterisk)) op = BinaryOperator.Multiply;
            else if (Match(TokenType.Slash)) op = BinaryOperator.Divide;
            else if (Match(TokenType.Percent)) op = BinaryOperator.Modulo;

            if (!op.HasValue) break;

            var right = ParseUnaryExpression();
            left = new BinaryExpression
            {
                Left = left,
                Operator = op.Value,
                Right = right
            };
        }

        return left;
    }

    private Expression ParseUnaryExpression()
    {
        if (Match(TokenType.Minus))
        {
            return new UnaryExpression
            {
                Operator = UnaryOperator.Negate,
                Operand = ParseUnaryExpression()
            };
        }

        // BINARY keyword for case-sensitive comparison - just pass through for compatibility
        // CyscaleDB treats it as a no-op since we don't have collation-based comparisons
        if (Match(TokenType.BINARY))
        {
            return ParseUnaryExpression();
        }

        return ParsePostfixExpression();
    }

    private Expression ParsePostfixExpression()
    {
        var expr = ParsePrimaryExpression();

        // Handle JSON path operators -> and ->>
        while (true)
        {
            if (Match(TokenType.Arrow))
            {
                // -> operator: JSON_EXTRACT
                var path = ParsePrimaryExpression();
                expr = new FunctionCall
                {
                    FunctionName = "JSON_EXTRACT",
                    Arguments = [expr, path]
                };
            }
            else if (Match(TokenType.DoubleArrow))
            {
                // ->> operator: JSON_UNQUOTE(JSON_EXTRACT(...))
                var path = ParsePrimaryExpression();
                var extractCall = new FunctionCall
                {
                    FunctionName = "JSON_EXTRACT",
                    Arguments = [expr, path]
                };
                expr = new FunctionCall
                {
                    FunctionName = "JSON_UNQUOTE",
                    Arguments = [extractCall]
                };
            }
            else
            {
                break;
            }
        }

        return expr;
    }

    private Expression ParsePrimaryExpression()
    {
        // Parenthesized expression or subquery
        if (Check(TokenType.LeftParen))
        {
            Advance();

            if (Check(TokenType.SELECT))
            {
                var subquery = ParseSelectStatement();
                Expect(TokenType.RightParen);
                return new Subquery { Query = subquery };
            }

            var expr = ParseExpression();
            Expect(TokenType.RightParen);
            return expr;
        }

        // EXISTS
        if (Match(TokenType.EXISTS))
        {
            Expect(TokenType.LeftParen);
            var subquery = ParseSelectStatement();
            Expect(TokenType.RightParen);
            return new ExistsExpression { Subquery = subquery };
        }

        // MATCH...AGAINST full-text search
        if (Match(TokenType.MATCH))
        {
            return ParseMatchExpression();
        }

        // CASE expression
        if (Match(TokenType.CASE))
        {
            return ParseCaseExpression();
        }

        // NULL
        if (Match(TokenType.NULL))
        {
            return new LiteralExpression { Value = DataValue.Null };
        }

        // Boolean literals
        if (Match(TokenType.TRUE))
        {
            return new LiteralExpression { Value = DataValue.FromBoolean(true) };
        }
        if (Match(TokenType.FALSE))
        {
            return new LiteralExpression { Value = DataValue.FromBoolean(false) };
        }

        // Integer literal
        if (Check(TokenType.IntegerLiteral))
        {
            var value = long.Parse(_currentToken.Value);
            Advance();
            
            if (value >= int.MinValue && value <= int.MaxValue)
            {
                return new LiteralExpression { Value = DataValue.FromInt((int)value) };
            }
            return new LiteralExpression { Value = DataValue.FromBigInt(value) };
        }

        // Float literal
        if (Check(TokenType.FloatLiteral))
        {
            var value = double.Parse(_currentToken.Value);
            Advance();
            return new LiteralExpression { Value = DataValue.FromDouble(value) };
        }

        // String literal
        if (Check(TokenType.StringLiteral))
        {
            var value = _currentToken.Value;
            Advance();
            return new LiteralExpression { Value = DataValue.FromVarChar(value) };
        }

        // System variable (@@variable_name)
        if (Check(TokenType.AtAt))
        {
            Advance();
            var varName = ExpectIdentifierOrKeyword();
            var scope = SetScope.Session;

            // Check for @@global.xxx or @@session.xxx
            if (varName.Equals("global", StringComparison.OrdinalIgnoreCase) && Match(TokenType.Dot))
            {
                scope = SetScope.Global;
                varName = ExpectIdentifierOrKeyword();
            }
            else if (varName.Equals("session", StringComparison.OrdinalIgnoreCase) && Match(TokenType.Dot))
            {
                scope = SetScope.Session;
                varName = ExpectIdentifierOrKeyword();
            }

            return new SystemVariableExpression { VariableName = varName, Scope = scope };
        }

        // Identifier (column or function call) - accept identifiers or keywords
        if (Check(TokenType.Identifier) || IsAggregateFunction() || IsKeyword(_currentToken.Type))
        {
            var name = _currentToken.Value;
            Advance();

            // Check for function call
            if (Check(TokenType.LeftParen))
            {
                return ParseFunctionCall(name);
            }

            // Check for qualified column (table.column)
            if (Match(TokenType.Dot))
            {
                var columnName = ExpectIdentifier();
                return new ColumnReference { TableName = name, ColumnName = columnName };
            }

            return new ColumnReference { ColumnName = name };
        }

        throw Error($"Unexpected token in expression: {_currentToken.Value}");
    }

    private bool IsAggregateFunction()
    {
        return _currentToken.Type == TokenType.COUNT ||
               _currentToken.Type == TokenType.SUM ||
               _currentToken.Type == TokenType.AVG ||
               _currentToken.Type == TokenType.MIN ||
               _currentToken.Type == TokenType.MAX;
    }

    private Expression ParseFunctionCall(string functionName)
    {
        var func = new FunctionCall { FunctionName = functionName.ToUpperInvariant() };
        Expect(TokenType.LeftParen);

        if (Match(TokenType.DISTINCT))
        {
            func.IsDistinct = true;
        }

        if (Match(TokenType.Asterisk))
        {
            func.IsStarArgument = true;
        }
        else if (!Check(TokenType.RightParen))
        {
            func.Arguments = ParseExpressionList();
        }

        // MySQL supports ORDER BY inside aggregate functions (e.g., GROUP_CONCAT(column ORDER BY column))
        // This is a MySQL extension, not standard SQL
        if (Check(TokenType.ORDER))
        {
            Advance();
            Expect(TokenType.BY);
            // Parse ORDER BY expression list (simplified - just parse expressions)
            // Note: This is a MySQL extension and may not be fully supported in execution
            func.OrderBy = ParseExpressionList();
        }

        // MySQL GROUP_CONCAT supports SEPARATOR clause (e.g., GROUP_CONCAT(column SEPARATOR ','))
        if (Check(TokenType.SEPARATOR))
        {
            Advance();
            if (!Check(TokenType.StringLiteral))
            {
                throw Error($"Expected string literal after SEPARATOR, got: {_currentToken.Value}");
            }
            func.Separator = _currentToken.Value;
            Advance();
        }

        Expect(TokenType.RightParen);

        // Check for OVER clause (window function)
        if (Check(TokenType.OVER))
        {
            return ParseWindowFunctionFromFunctionCall(func);
        }

        return func;
    }

    /// <summary>
    /// Converts a FunctionCall to a WindowFunctionCall with OVER clause.
    /// </summary>
    private WindowFunctionCall ParseWindowFunctionFromFunctionCall(FunctionCall func)
    {
        Expect(TokenType.OVER);

        var windowFunc = new WindowFunctionCall
        {
            FunctionName = func.FunctionName,
            Arguments = func.Arguments,
            WindowSpec = ParseWindowSpec()
        };

        return windowFunc;
    }

    /// <summary>
    /// Parses a window specification (OVER clause content).
    /// Syntax: OVER ([window_name] [PARTITION BY ...] [ORDER BY ...] [frame_clause])
    /// </summary>
    private WindowSpec ParseWindowSpec()
    {
        var spec = new WindowSpec();

        Expect(TokenType.LeftParen);

        // Check for window name reference
        if (Check(TokenType.Identifier) && !Check(TokenType.PARTITION) && !Check(TokenType.ORDER))
        {
            spec.WindowName = ExpectIdentifier();
        }

        // PARTITION BY clause
        if (Check(TokenType.PARTITION))
        {
            Advance();
            Expect(TokenType.BY);
            spec.PartitionBy = ParseExpressionList();
        }

        // ORDER BY clause
        if (Check(TokenType.ORDER))
        {
            Advance();
            Expect(TokenType.BY);
            spec.OrderBy = ParseOrderByList();
        }

        // Frame clause (ROWS or RANGE)
        if (Check(TokenType.ROWS) || Check(TokenType.RANGE))
        {
            spec.Frame = ParseWindowFrame();
        }

        Expect(TokenType.RightParen);

        return spec;
    }

    /// <summary>
    /// Parses a window frame clause.
    /// Syntax: (ROWS|RANGE) (frame_start | BETWEEN frame_start AND frame_end)
    /// </summary>
    private WindowFrame ParseWindowFrame()
    {
        var frame = new WindowFrame();

        // Frame type
        if (Match(TokenType.ROWS))
        {
            frame.FrameType = WindowFrameType.Rows;
        }
        else if (Match(TokenType.RANGE))
        {
            frame.FrameType = WindowFrameType.Range;
        }
        else
        {
            throw Error($"Expected ROWS or RANGE, got: {_currentToken.Value}");
        }

        // Frame bounds
        if (Match(TokenType.BETWEEN))
        {
            frame.Start = ParseWindowFrameBound();
            Expect(TokenType.AND);
            frame.End = ParseWindowFrameBound();
        }
        else
        {
            frame.Start = ParseWindowFrameBound();
            // Default end is CURRENT ROW when not using BETWEEN
            frame.End = new WindowFrameBound { BoundType = WindowFrameBoundType.CurrentRow };
        }

        return frame;
    }

    /// <summary>
    /// Parses a window frame bound.
    /// </summary>
    private WindowFrameBound ParseWindowFrameBound()
    {
        var bound = new WindowFrameBound();

        if (Match(TokenType.UNBOUNDED))
        {
            if (Match(TokenType.PRECEDING))
            {
                bound.BoundType = WindowFrameBoundType.UnboundedPreceding;
            }
            else if (Match(TokenType.FOLLOWING))
            {
                bound.BoundType = WindowFrameBoundType.UnboundedFollowing;
            }
            else
            {
                throw Error($"Expected PRECEDING or FOLLOWING after UNBOUNDED, got: {_currentToken.Value}");
            }
        }
        else if (Check(TokenType.CURRENT))
        {
            Advance();
            Expect(TokenType.ROW);
            bound.BoundType = WindowFrameBoundType.CurrentRow;
        }
        else if (Check(TokenType.IntegerLiteral))
        {
            bound.Offset = ParseInteger();
            if (Match(TokenType.PRECEDING))
            {
                bound.BoundType = WindowFrameBoundType.Preceding;
            }
            else if (Match(TokenType.FOLLOWING))
            {
                bound.BoundType = WindowFrameBoundType.Following;
            }
            else
            {
                throw Error($"Expected PRECEDING or FOLLOWING after offset, got: {_currentToken.Value}");
            }
        }
        else
        {
            throw Error($"Invalid window frame bound: {_currentToken.Value}");
        }

        return bound;
    }

    /// <summary>
    /// Parses a CASE expression.
    /// Supports both simple CASE (CASE expr WHEN val THEN result...)
    /// and searched CASE (CASE WHEN condition THEN result...).
    /// </summary>
    private CaseExpression ParseCaseExpression()
    {
        var caseExpr = new CaseExpression();

        // Check if this is a simple CASE (has an operand)
        // Simple CASE: CASE expr WHEN value THEN result
        // Searched CASE: CASE WHEN condition THEN result
        if (!Check(TokenType.WHEN))
        {
            // This is a simple CASE - parse the operand
            caseExpr.Operand = ParseExpression();
        }

        // Parse WHEN clauses (at least one required)
        while (Match(TokenType.WHEN))
        {
            var whenClause = new WhenClause();
            whenClause.When = ParseExpression();

            Expect(TokenType.THEN);
            whenClause.Then = ParseExpression();

            caseExpr.WhenClauses.Add(whenClause);
        }

        if (caseExpr.WhenClauses.Count == 0)
        {
            throw Error("CASE expression requires at least one WHEN clause");
        }

        // Parse optional ELSE clause
        if (Match(TokenType.ELSE))
        {
            caseExpr.ElseResult = ParseExpression();
        }

        // END is required
        Expect(TokenType.END);

        return caseExpr;
    }

    /// <summary>
    /// Parses MATCH(columns) AGAINST(search_text [mode]) expression.
    /// </summary>
    private MatchExpression ParseMatchExpression()
    {
        var matchExpr = new MatchExpression();

        // Parse column list: MATCH(col1, col2, ...)
        Expect(TokenType.LeftParen);
        
        do
        {
            var name = ExpectIdentifier();
            ColumnReference colRef;
            
            // Check for table.column syntax
            if (Match(TokenType.Dot))
            {
                var columnName = ExpectIdentifier();
                colRef = new ColumnReference { TableName = name, ColumnName = columnName };
            }
            else
            {
                colRef = new ColumnReference { ColumnName = name };
            }
            
            matchExpr.Columns.Add(colRef);
        } while (Match(TokenType.Comma));

        Expect(TokenType.RightParen);

        // Parse AGAINST clause
        Expect(TokenType.AGAINST);
        Expect(TokenType.LeftParen);

        // Parse search text
        matchExpr.SearchText = ParseExpression();

        // Parse optional mode modifiers
        if (Match(TokenType.IN))
        {
            // IN NATURAL LANGUAGE MODE [WITH QUERY EXPANSION]
            // IN BOOLEAN MODE
            if (Match(TokenType.NATURAL))
            {
                Expect(TokenType.LANGUAGE);
                Expect(TokenType.MODE);
                matchExpr.Mode = MatchSearchMode.NaturalLanguage;

                // Check for WITH QUERY EXPANSION
                if (Match(TokenType.WITH))
                {
                    // QUERY should be an identifier
                    var queryWord = ExpectIdentifierOrKeyword();
                    if (!queryWord.Equals("QUERY", StringComparison.OrdinalIgnoreCase))
                    {
                        throw Error($"Expected 'QUERY', got '{queryWord}'");
                    }
                    Expect(TokenType.EXPANSION);
                    matchExpr.Mode = MatchSearchMode.NaturalLanguageWithQueryExpansion;
                    matchExpr.WithQueryExpansion = true;
                }
            }
            else if (Match(TokenType.BOOLEAN))
            {
                Expect(TokenType.MODE);
                matchExpr.Mode = MatchSearchMode.Boolean;
            }
            else
            {
                throw Error("Expected NATURAL or BOOLEAN after IN");
            }
        }
        else if (Match(TokenType.WITH))
        {
            // WITH QUERY EXPANSION (without IN ... MODE)
            var queryWord = ExpectIdentifierOrKeyword();
            if (!queryWord.Equals("QUERY", StringComparison.OrdinalIgnoreCase))
            {
                throw Error($"Expected 'QUERY', got '{queryWord}'");
            }
            Expect(TokenType.EXPANSION);
            matchExpr.WithQueryExpansion = true;
        }

        Expect(TokenType.RightParen);

        return matchExpr;
    }

    #endregion

    #region User Management Statements

    private GrantStatement ParseGrantStatement()
    {
        Expect(TokenType.GRANT);

        var stmt = new GrantStatement();

        // Parse privileges
        if (Match(TokenType.ALL))
        {
            Match(TokenType.PRIVILEGES); // Optional
            stmt.Privileges.Add("ALL");
        }
        else
        {
            do
            {
                stmt.Privileges.Add(ExpectIdentifier());
            } while (Match(TokenType.Comma));
        }

        Expect(TokenType.ON);

        // Parse database.table or table
        var firstIdentifier = ExpectIdentifier();
        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = firstIdentifier;
            stmt.TableName = ExpectIdentifier();
        }
        else
        {
            stmt.TableName = firstIdentifier;
        }

        Expect(TokenType.TO);

        // Parse username@host
        stmt.UserName = ExpectIdentifier();
        if (Match(TokenType.AtAt) || (_currentToken.Value == "@" && Check(TokenType.Identifier)))
        {
            if (!Check(TokenType.AtAt))
                Advance(); // Skip @
            stmt.Host = ExpectIdentifier();
        }

        return stmt;
    }

    private RevokeStatement ParseRevokeStatement()
    {
        Expect(TokenType.REVOKE);

        var stmt = new RevokeStatement();

        // Parse privileges
        if (Match(TokenType.ALL))
        {
            Match(TokenType.PRIVILEGES); // Optional
            stmt.Privileges.Add("ALL");
        }
        else
        {
            do
            {
                stmt.Privileges.Add(ExpectIdentifier());
            } while (Match(TokenType.Comma));
        }

        Expect(TokenType.ON);

        // Parse database.table or table
        var firstIdentifier = ExpectIdentifier();
        if (Match(TokenType.Dot))
        {
            stmt.DatabaseName = firstIdentifier;
            stmt.TableName = ExpectIdentifier();
        }
        else
        {
            stmt.TableName = firstIdentifier;
        }

        // FROM keyword
        if (MatchIdentifier("FROM"))
        {
            // Parse username@host
            stmt.UserName = ExpectIdentifier();
            if (Match(TokenType.AtAt) || (_currentToken.Value == "@" && Check(TokenType.Identifier)))
            {
                if (!Check(TokenType.AtAt))
                    Advance(); // Skip @
                stmt.Host = ExpectIdentifier();
            }
        }

        return stmt;
    }

    #endregion

    #region Helper Methods

    private List<Expression> ParseExpressionList()
    {
        var list = new List<Expression>();

        do
        {
            list.Add(ParseExpression());
        } while (Match(TokenType.Comma));

        return list;
    }

    private List<string> ParseIdentifierList()
    {
        var list = new List<string>();

        do
        {
            list.Add(ExpectIdentifier());
        } while (Match(TokenType.Comma));

        return list;
    }

    private int ParseInteger()
    {
        if (!Check(TokenType.IntegerLiteral))
        {
            throw Error($"Expected integer, got: {_currentToken.Value}");
        }

        var value = int.Parse(_currentToken.Value);
        Advance();
        return value;
    }

    private string ExpectIdentifier()
    {
        // Accept identifier or keyword (keywords can be used as identifiers in MySQL)
        if (Check(TokenType.Identifier))
        {
            var value = _currentToken.Value;
            Advance();
            return value;
        }

        // Accept keywords as identifiers (for table names like TABLES, COLUMNS, etc.)
        if (IsKeyword(_currentToken.Type))
        {
            var value = _currentToken.Value;
            Advance();
            return value;
        }

        throw Error($"Expected identifier, got: {_currentToken.Value}");
    }

    private void Expect(TokenType type)
    {
        if (!Check(type))
        {
            throw Error($"Expected {type}, got: {_currentToken.Type} ({_currentToken.Value})");
        }
        Advance();
    }

    private bool Check(TokenType type)
    {
        return _currentToken.Type == type;
    }

    private bool Match(TokenType type)
    {
        if (Check(type))
        {
            Advance();
            return true;
        }
        return false;
    }

    private Token Peek()
    {
        return _lexer.Peek();
    }

    private void Advance()
    {
        _currentToken = _lexer.NextToken();
    }

    private static bool IsKeyword(TokenType type)
    {
        // Check if it's a keyword (all keywords are >= SELECT and < AtAt)
        // AtAt is the last non-keyword token before keywords start
        return type >= TokenType.SELECT && type < TokenType.AtAt;
    }

    private SqlSyntaxException Error(string message)
    {
        return new SqlSyntaxException(message, _currentToken.Position, _currentToken.Line, _currentToken.Column);
    }

    #endregion

    #region Stored Procedures, Functions, Triggers, Events (Phase 3 - Stubs)

    // These are stub implementations for Phase 3 features to allow compilation
    // They will be fully implemented in Phase 3

    private Statement ParseCreateProcedureStatement()
    {
        // CREATE [OR REPLACE] PROCEDURE name ([param [, param] ...]) [characteristics] body
        // Note: OR REPLACE might have already been consumed by ParseCreateStatement
        var stmt = new CreateProcedureStatement();

        Expect(TokenType.PROCEDURE);

        // Procedure name
        stmt.ProcedureName = ExpectIdentifier();

        // Parameters
        Expect(TokenType.LeftParen);
        
        if (!Check(TokenType.RightParen))
        {
            do
            {
                var param = new ProcedureParameter();

                // Parameter mode (IN, OUT, INOUT) - default is IN
                if (Check(TokenType.IN) || MatchIdentifier("IN"))
                {
                    Advance();
                    param.Mode = ParameterMode.In;
                }
                else if (Check(TokenType.OUT))
                {
                    Advance();
                    param.Mode = ParameterMode.Out;
                }
                else if (Check(TokenType.INOUT))
                {
                    Advance();
                    param.Mode = ParameterMode.InOut;
                }

                // Parameter name
                param.Name = ExpectIdentifier();

                // Parameter type
                param.DataType = ParseDataType(out var length, out var precision, out var scale);
                // For parameters, use length/precision interchangeably as "size"
                param.Size = length ?? precision;
                param.Scale = scale;

                stmt.Parameters.Add(param);
            } while (Match(TokenType.Comma));
        }

        Expect(TokenType.RightParen);

        // Optional characteristics
        while (true)
        {
            if (MatchIdentifier("COMMENT"))
            {
                stmt.Comment = ExpectStringLiteral();
            }
            else if (MatchIdentifier("LANGUAGE"))
            {
                Expect(TokenType.SQL);
            }
            else if (Check(TokenType.DETERMINISTIC))
            {
                Advance();
                // Note: This is typically for functions, but MySQL allows it for procedures
            }
            else if (MatchIdentifier("NOT"))
            {
                // Check if next token is DETERMINISTIC
                if (Check(TokenType.DETERMINISTIC))
                {
                    Advance(); // DETERMINISTIC
                }
                else
                {
                    throw Error("Expected DETERMINISTIC after NOT");
                }
            }
            else if (Check(TokenType.CONTAINS))
            {
                Advance();
                Expect(TokenType.SQL);
            }
            else if (MatchIdentifier("NO"))
            {
                Advance();
                Expect(TokenType.SQL);
            }
            else if (MatchIdentifier("READS"))
            {
                Advance();
                Expect(TokenType.SQL);
                MatchIdentifier("DATA"); // Optional DATA keyword
            }
            else if (MatchIdentifier("MODIFIES"))
            {
                Advance();
                Expect(TokenType.SQL);
                MatchIdentifier("DATA"); // Optional DATA keyword
            }
            else if (Check(TokenType.SQL))
            {
                Advance();
                Expect(TokenType.SECURITY);
                if (Check(TokenType.DEFINER))
                {
                    Advance();
                    stmt.SqlSecurity = "DEFINER";
                }
                else if (Check(TokenType.INVOKER))
                {
                    Advance();
                    stmt.SqlSecurity = "INVOKER";
                }
                else
                {
                    throw Error("Expected DEFINER or INVOKER after SQL SECURITY");
                }
            }
            else if (Check(TokenType.DEFINER))
            {
                Advance();
                Expect(TokenType.Equal);
                stmt.Definer = ParseDefinerClause();
            }
            else
            {
                break; // No more characteristics
            }
        }

        // Procedure body - must be BEGIN...END block
        stmt.Body = ParseProcedureBody();

        return stmt;
    }

    /// <summary>
    /// Parses a definer clause (user@host or CURRENT_USER).
    /// </summary>
    private string ParseDefinerClause()
    {
        if (MatchIdentifier("CURRENT_USER"))
        {
            return "CURRENT_USER";
        }

        var user = ExpectIdentifierOrKeyword();
        if (Match(TokenType.AtAt))
        {
            var host = ExpectIdentifierOrKeyword();
            return $"{user}@{host}";
        }
        return user;
    }

    /// <summary>
    /// Parses a procedure body (BEGIN...END block with statements).
    /// </summary>
    private List<Statement> ParseProcedureBody()
    {
        // MySQL procedure body must be a BEGIN...END block
        Expect(TokenType.BEGIN);

        var statements = new List<Statement>();

        // Parse statements until we hit END
        while (!Check(TokenType.END))
        {
            if (Check(TokenType.EOF))
            {
                throw Error("Unexpected end of input in procedure body. Expected END.");
            }

            statements.Add(ParseProcedureStatement());

            // Consume optional semicolon between statements
            if (Check(TokenType.Semicolon))
            {
                Advance();
            }
        }

        Expect(TokenType.END);

        return statements;
    }

    /// <summary>
    /// Parses a statement within a procedure body.
    /// This includes all normal SQL statements plus procedure-specific control flow.
    /// </summary>
    private Statement ParseProcedureStatement()
    {
        // Procedure statements can be:
        // - Regular SQL statements (SELECT, INSERT, UPDATE, DELETE, etc.)
        // - Procedure control flow (DECLARE, SET, IF, WHILE, LOOP, etc.)
        // - Other procedure-specific statements (LEAVE, ITERATE, RETURN)
        
        return ParseStatement();
    }

    private Statement ParseCreateFunctionStatement()
    {
        // CREATE [OR REPLACE] FUNCTION name ([param [, param] ...])
        // RETURNS type [characteristics] body
        var stmt = new CreateFunctionStatement();

        Expect(TokenType.FUNCTION);

        // Function name
        stmt.FunctionName = ExpectIdentifier();

        // Parameters
        Expect(TokenType.LeftParen);
        
        if (!Check(TokenType.RightParen))
        {
            do
            {
                var param = new ProcedureParameter();

                // Functions don't support OUT/INOUT parameters, only IN
                // But we'll parse them for compatibility if present
                if (Check(TokenType.IN) || MatchIdentifier("IN"))
                {
                    Advance();
                    param.Mode = ParameterMode.In;
                }
                else if (Check(TokenType.OUT))
                {
                    Advance();
                    param.Mode = ParameterMode.Out;
                }
                else if (Check(TokenType.INOUT))
                {
                    Advance();
                    param.Mode = ParameterMode.InOut;
                }

                // Parameter name
                param.Name = ExpectIdentifier();

                // Parameter type
                param.DataType = ParseDataType(out var length, out var precision, out var scale);
                param.Size = length ?? precision;
                param.Scale = scale;

                stmt.Parameters.Add(param);
            } while (Match(TokenType.Comma));
        }

        Expect(TokenType.RightParen);

        // RETURNS clause - required for functions
        Expect(TokenType.RETURNS);
        stmt.ReturnType = ParseDataType(out var retLength, out var retPrecision, out var retScale);
        stmt.ReturnSize = retLength ?? retPrecision;
        stmt.ReturnScale = retScale;

        // Optional characteristics
        while (true)
        {
            if (MatchIdentifier("COMMENT"))
            {
                stmt.Comment = ExpectStringLiteral();
            }
            else if (MatchIdentifier("LANGUAGE"))
            {
                Expect(TokenType.SQL);
            }
            else if (Check(TokenType.DETERMINISTIC))
            {
                Advance();
                stmt.IsDeterministic = true;
            }
            else if (MatchIdentifier("NOT"))
            {
                // NOT DETERMINISTIC
                if (Check(TokenType.DETERMINISTIC))
                {
                    Advance();
                    stmt.IsDeterministic = false;
                }
                else
                {
                    throw Error("Expected DETERMINISTIC after NOT");
                }
            }
            else if (Check(TokenType.CONTAINS))
            {
                Advance();
                Expect(TokenType.SQL);
            }
            else if (MatchIdentifier("NO"))
            {
                Advance();
                Expect(TokenType.SQL);
            }
            else if (MatchIdentifier("READS"))
            {
                Advance();
                Expect(TokenType.SQL);
                MatchIdentifier("DATA"); // Optional DATA keyword
            }
            else if (MatchIdentifier("MODIFIES"))
            {
                Advance();
                Expect(TokenType.SQL);
                MatchIdentifier("DATA"); // Optional DATA keyword
            }
            else if (Check(TokenType.SQL))
            {
                Advance();
                Expect(TokenType.SECURITY);
                if (Check(TokenType.DEFINER))
                {
                    Advance();
                    stmt.SqlSecurity = "DEFINER";
                }
                else if (Check(TokenType.INVOKER))
                {
                    Advance();
                    stmt.SqlSecurity = "INVOKER";
                }
                else
                {
                    throw Error("Expected DEFINER or INVOKER after SQL SECURITY");
                }
            }
            else if (Check(TokenType.DEFINER))
            {
                Advance();
                Expect(TokenType.Equal);
                stmt.Definer = ParseDefinerClause();
            }
            else
            {
                break; // No more characteristics
            }
        }

        // Function body - must be BEGIN...END block
        stmt.Body = ParseProcedureBody();

        return stmt;
    }

    private Statement ParseCreateTriggerStatement()
    {
        // CREATE [OR REPLACE] TRIGGER trigger_name
        // {BEFORE | AFTER} {INSERT | UPDATE | DELETE}
        // ON table_name FOR EACH ROW
        // trigger_body

        Expect(TokenType.TRIGGER);

        var stmt = new CreateTriggerStatement();

        // Trigger name
        stmt.TriggerName = ExpectIdentifier();

        // Timing: BEFORE or AFTER
        if (Match(TokenType.BEFORE))
        {
            stmt.Timing = TriggerTiming.Before;
        }
        else if (Match(TokenType.AFTER))
        {
            stmt.Timing = TriggerTiming.After;
        }
        else
        {
            throw Error($"Expected BEFORE or AFTER, got: {_currentToken.Value}");
        }

        // Event: INSERT, UPDATE, or DELETE
        if (Match(TokenType.INSERT))
        {
            stmt.Event = TriggerEvent.Insert;
        }
        else if (Match(TokenType.UPDATE))
        {
            stmt.Event = TriggerEvent.Update;
        }
        else if (Match(TokenType.DELETE))
        {
            stmt.Event = TriggerEvent.Delete;
        }
        else
        {
            throw Error($"Expected INSERT, UPDATE, or DELETE, got: {_currentToken.Value}");
        }

        // ON table_name
        Expect(TokenType.ON);
        stmt.TableName = ExpectIdentifier();

        // FOR EACH ROW
        Expect(TokenType.FOR);
        ExpectIdentifierValue("EACH");
        ExpectIdentifierValue("ROW");

        // Parse trigger body (single statement or BEGIN...END block)
        if (Check(TokenType.BEGIN))
        {
            stmt.Body = ParseProcedureBody();
        }
        else
        {
            // Single statement
            stmt.Body = [ParseStatement()];
        }

        return stmt;
    }

    private Statement ParseCreateEventStatement()
    {
        // CREATE [OR REPLACE] EVENT event_name
        // ON SCHEDULE schedule_expr
        // [ON COMPLETION [NOT] PRESERVE]
        // [ENABLE | DISABLE]
        // [COMMENT 'comment']
        // DO event_body

        // EVENT keyword (may have been consumed by MatchIdentifier)
        if (Check(TokenType.EVENT))
            Advance();

        var stmt = new CreateEventStatement();

        // Event name
        stmt.EventName = ExpectIdentifier();

        // ON SCHEDULE
        Expect(TokenType.ON);
        Expect(TokenType.SCHEDULE);

        // Parse schedule expression (simplified - store as string)
        stmt.Schedule = ParseScheduleExpression();

        // Optional: ON COMPLETION [NOT] PRESERVE
        if (Check(TokenType.ON))
        {
            Advance();
            ExpectIdentifierValue("COMPLETION");
            if (Match(TokenType.NOT))
            {
                ExpectIdentifierValue("PRESERVE");
                stmt.OnCompletionPreserve = false;
            }
            else
            {
                ExpectIdentifierValue("PRESERVE");
                stmt.OnCompletionPreserve = true;
            }
        }

        // Optional: ENABLE or DISABLE
        if (Match(TokenType.ENABLE))
        {
            stmt.Enabled = true;
        }
        else if (Match(TokenType.DISABLE))
        {
            stmt.Enabled = false;
        }

        // Optional: COMMENT
        if (MatchIdentifier("COMMENT"))
        {
            if (Check(TokenType.StringLiteral))
            {
                stmt.Comment = _currentToken.Value;
                Advance();
            }
        }

        // DO event_body
        Expect(TokenType.DO);

        // Parse event body (single statement or BEGIN...END block)
        if (Check(TokenType.BEGIN))
        {
            stmt.Body = ParseProcedureBody();
        }
        else
        {
            stmt.Body = [ParseStatement()];
        }

        return stmt;
    }

    /// <summary>
    /// Parses a schedule expression (EVERY interval or AT timestamp).
    /// Returns the schedule as a string representation.
    /// </summary>
    private string ParseScheduleExpression()
    {
        var parts = new List<string>();

        // EVERY interval or AT timestamp
        if (Match(TokenType.EVERY))
        {
            parts.Add("EVERY");

            // Parse interval value
            if (Check(TokenType.IntegerLiteral))
            {
                parts.Add(_currentToken.Value);
                Advance();
            }

            // Parse interval unit (SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR)
            if (Check(TokenType.Identifier))
            {
                parts.Add(_currentToken.Value.ToUpperInvariant());
                Advance();
            }

            // Optional: STARTS timestamp
            if (MatchIdentifier("STARTS"))
            {
                parts.Add("STARTS");
                if (Check(TokenType.StringLiteral))
                {
                    parts.Add($"'{_currentToken.Value}'");
                    Advance();
                }
            }

            // Optional: ENDS timestamp
            if (MatchIdentifier("ENDS"))
            {
                parts.Add("ENDS");
                if (Check(TokenType.StringLiteral))
                {
                    parts.Add($"'{_currentToken.Value}'");
                    Advance();
                }
            }
        }
        else if (Match(TokenType.AT))
        {
            parts.Add("AT");
            if (Check(TokenType.StringLiteral))
            {
                parts.Add($"'{_currentToken.Value}'");
                Advance();
            }
        }

        return string.Join(" ", parts);
    }

    private Statement ParseDropProcedureStatement()
    {
        // DROP PROCEDURE [IF EXISTS] procedure_name
        Expect(TokenType.PROCEDURE);

        var stmt = new DropProcedureStatement();

        // Check for IF EXISTS
        if (Match(TokenType.IF))
        {
            Expect(TokenType.EXISTS);
            stmt.IfExists = true;
        }

        // Procedure name
        stmt.ProcedureName = ExpectIdentifier();

        return stmt;
    }

    private Statement ParseDropFunctionStatement()
    {
        throw new NotImplementedException("DROP FUNCTION is not yet implemented (Phase 3)");
    }

    private Statement ParseDropTriggerStatement()
    {
        // DROP TRIGGER [IF EXISTS] trigger_name
        Expect(TokenType.TRIGGER);

        var stmt = new DropTriggerStatement();

        // Check for IF EXISTS
        if (Match(TokenType.IF))
        {
            Expect(TokenType.EXISTS);
            stmt.IfExists = true;
        }

        // Trigger name
        stmt.TriggerName = ExpectIdentifier();

        return stmt;
    }

    private Statement ParseDropEventStatement()
    {
        // DROP EVENT [IF EXISTS] event_name
        Expect(TokenType.EVENT);

        var stmt = new DropEventStatement();

        // Check for IF EXISTS
        if (Match(TokenType.IF))
        {
            Expect(TokenType.EXISTS);
            stmt.IfExists = true;
        }

        // Event name
        stmt.EventName = ExpectIdentifier();

        return stmt;
    }

    #region Admin Statements

    private Statement ParseAnalyzeStatement()
    {
        // ANALYZE TABLE table_name [, table_name] ...
        Expect(TokenType.ANALYZE);
        Expect(TokenType.TABLE);

        var stmt = new AnalyzeTableStatement();

        do
        {
            stmt.TableNames.Add(ExpectIdentifier());
        } while (Match(TokenType.Comma));

        return stmt;
    }

    private Statement ParseFlushStatement()
    {
        // FLUSH [TABLES [table_name [, table_name] ...] [WITH READ LOCK] | PRIVILEGES | LOGS]
        Expect(TokenType.FLUSH);

        var stmt = new FlushStatement();

        if (Match(TokenType.TABLES))
        {
            stmt.FlushType = "TABLES";

            // Optional table names
            if (Check(TokenType.Identifier))
            {
                do
                {
                    stmt.TableNames.Add(ExpectIdentifier());
                } while (Match(TokenType.Comma));
            }

            // Optional WITH READ LOCK
            if (Match(TokenType.WITH))
            {
                Expect(TokenType.READ);
                Expect(TokenType.LOCK);
                stmt.WithReadLock = true;
            }
        }
        else if (Match(TokenType.PRIVILEGES))
        {
            stmt.FlushType = "PRIVILEGES";
        }
        else if (MatchIdentifier("LOGS"))
        {
            stmt.FlushType = "LOGS";
        }
        else if (MatchIdentifier("STATUS"))
        {
            stmt.FlushType = "STATUS";
        }
        else
        {
            throw Error($"Expected TABLES, PRIVILEGES, LOGS, or STATUS after FLUSH, got: {_currentToken.Value}");
        }

        return stmt;
    }

    private Statement ParseLockTablesStatement()
    {
        // LOCK TABLES table_name [[AS] alias] lock_type [, ...]
        Expect(TokenType.LOCK);
        Expect(TokenType.TABLES);

        var stmt = new LockTablesStatement();

        do
        {
            var tableLock = new TableLock
            {
                TableName = ExpectIdentifier()
            };

            // Optional alias
            if (Match(TokenType.AS))
            {
                tableLock.Alias = ExpectIdentifier();
            }
            else if (Check(TokenType.Identifier) && !IsLockType(_currentToken.Value))
            {
                tableLock.Alias = ExpectIdentifier();
            }

            // Lock type
            if (Match(TokenType.READ))
            {
                if (MatchIdentifier("LOCAL"))
                {
                    tableLock.LockType = "READ LOCAL";
                }
                else
                {
                    tableLock.LockType = "READ";
                }
            }
            else if (Match(TokenType.WRITE))
            {
                tableLock.LockType = "WRITE";
            }
            else if (MatchIdentifier("LOW_PRIORITY"))
            {
                Expect(TokenType.WRITE);
                tableLock.LockType = "LOW_PRIORITY WRITE";
            }
            else
            {
                throw Error($"Expected READ or WRITE lock type, got: {_currentToken.Value}");
            }

            stmt.TableLocks.Add(tableLock);
        } while (Match(TokenType.Comma));

        return stmt;
    }

    private static bool IsLockType(string value)
    {
        return value.Equals("READ", StringComparison.OrdinalIgnoreCase) ||
               value.Equals("WRITE", StringComparison.OrdinalIgnoreCase) ||
               value.Equals("LOW_PRIORITY", StringComparison.OrdinalIgnoreCase);
    }

    private Statement ParseUnlockTablesStatement()
    {
        // UNLOCK TABLES
        Expect(TokenType.UNLOCK);
        Expect(TokenType.TABLES);

        return new UnlockTablesStatement();
    }

    private Statement ParseExplainStatement()
    {
        // EXPLAIN [ANALYZE] [FORMAT = {TRADITIONAL | JSON | TREE}] statement
        Expect(TokenType.EXPLAIN);

        var stmt = new ExplainStatement();

        // Check for ANALYZE
        if (MatchIdentifier("ANALYZE"))
        {
            stmt.Analyze = true;
        }

        // Check for FORMAT
        if (MatchIdentifier("FORMAT"))
        {
            Match(TokenType.Equal);  // Optional = sign

            if (MatchIdentifier("TRADITIONAL"))
            {
                stmt.Format = ExplainFormat.Traditional;
            }
            else if (MatchIdentifier("JSON"))
            {
                stmt.Format = ExplainFormat.Json;
            }
            else if (MatchIdentifier("TREE"))
            {
                stmt.Format = ExplainFormat.Tree;
            }
            else
            {
                throw Error($"Expected TRADITIONAL, JSON, or TREE after FORMAT =, got: {_currentToken.Value}");
            }
        }

        // Parse the statement to explain
        stmt.Statement = ParseStatement();

        return stmt;
    }

    #endregion

    private Statement ParseCallStatement()
    {
        // CALL procedure_name([argument [, argument] ...])
        Expect(TokenType.CALL);

        var stmt = new CallStatement
        {
            ProcedureName = ExpectIdentifier()
        };

        Expect(TokenType.LeftParen);

        // Parse arguments
        if (!Check(TokenType.RightParen))
        {
            do
            {
                stmt.Arguments.Add(ParseExpression());
            } while (Match(TokenType.Comma));
        }

        Expect(TokenType.RightParen);

        return stmt;
    }

    private Statement ParseDeclareVariableStatement()
    {
        // DECLARE var_name [, var_name] ... type [DEFAULT value]
        Expect(TokenType.DECLARE);

        var stmt = new DeclareVariableStatement();

        // Parse variable names
        do
        {
            stmt.VariableNames.Add(ExpectIdentifier());
        } while (Match(TokenType.Comma));

        // Parse data type
        stmt.DataType = ParseDataType(out var length, out var precision, out var scale);
        // For variables, use length/precision interchangeably as "size"
        stmt.Size = length ?? precision;
        stmt.Scale = scale;

        // Optional DEFAULT clause
        if (Match(TokenType.DEFAULT))
        {
            stmt.DefaultValue = ParseExpression();
        }

        return stmt;
    }

    private Statement ParseIfStatement()
    {
        // IF condition THEN statements [ELSEIF condition THEN statements] ... [ELSE statements] END IF
        Expect(TokenType.IF);

        var stmt = new IfStatement();

        // Parse the main IF condition
        stmt.Condition = ParseExpression();

        // THEN keyword
        Expect(TokenType.THEN);

        // Parse THEN statements
        while (!Check(TokenType.ELSEIF) && !Check(TokenType.ELSE) && !Check(TokenType.END))
        {
            if (Check(TokenType.EOF))
            {
                throw Error("Unexpected end of input in IF statement. Expected END IF.");
            }

            stmt.ThenStatements.Add(ParseProcedureStatement());

            // Consume optional semicolon between statements
            if (Check(TokenType.Semicolon))
            {
                Advance();
            }
        }

        // Parse ELSEIF clauses
        while (Check(TokenType.ELSEIF))
        {
            Advance(); // ELSEIF

            var elseIfCondition = ParseExpression();
            Expect(TokenType.THEN);

            var elseIfStatements = new List<Statement>();
            while (!Check(TokenType.ELSEIF) && !Check(TokenType.ELSE) && !Check(TokenType.END))
            {
                if (Check(TokenType.EOF))
                {
                    throw Error("Unexpected end of input in ELSEIF clause. Expected END IF.");
                }

                elseIfStatements.Add(ParseProcedureStatement());

                // Consume optional semicolon between statements
                if (Check(TokenType.Semicolon))
                {
                    Advance();
                }
            }

            stmt.ElseIfClauses.Add((elseIfCondition, elseIfStatements));
        }

        // Parse optional ELSE clause
        if (Check(TokenType.ELSE))
        {
            Advance(); // ELSE

            stmt.ElseStatements = new List<Statement>();
            while (!Check(TokenType.END))
            {
                if (Check(TokenType.EOF))
                {
                    throw Error("Unexpected end of input in ELSE clause. Expected END IF.");
                }

                stmt.ElseStatements.Add(ParseProcedureStatement());

                // Consume optional semicolon between statements
                if (Check(TokenType.Semicolon))
                {
                    Advance();
                }
            }
        }

        // END IF
        Expect(TokenType.END);
        Expect(TokenType.IF);

        return stmt;
    }

    private Statement ParseWhileStatement()
    {
        // [label:] WHILE condition DO statements END WHILE [label]
        string? label = null;

        // Check if there's a label before WHILE
        // This would have been parsed as an identifier before we got here
        // For now, we'll just parse the WHILE statement itself
        
        Expect(TokenType.WHILE);

        var stmt = new WhileStatement();

        // Parse the condition
        stmt.Condition = ParseExpression();

        // DO keyword
        Expect(TokenType.DO);

        // Parse loop body statements
        while (!Check(TokenType.END))
        {
            if (Check(TokenType.EOF))
            {
                throw Error("Unexpected end of input in WHILE loop. Expected END WHILE.");
            }

            stmt.Body.Add(ParseProcedureStatement());

            // Consume optional semicolon between statements
            if (Check(TokenType.Semicolon))
            {
                Advance();
            }
        }

        // END WHILE
        Expect(TokenType.END);
        Expect(TokenType.WHILE);

        // Optional label after END WHILE
        if (Check(TokenType.Identifier))
        {
            stmt.Label = _currentToken.Value;
            Advance();
        }

        return stmt;
    }

    private Statement ParseRepeatStatement()
    {
        throw new NotImplementedException("REPEAT statement is not yet implemented (Phase 3)");
    }

    private Statement ParseLoopStatement()
    {
        throw new NotImplementedException("LOOP statement is not yet implemented (Phase 3)");
    }

    private Statement ParseLeaveStatement()
    {
        throw new NotImplementedException("LEAVE statement is not yet implemented (Phase 3)");
    }

    private Statement ParseIterateStatement()
    {
        throw new NotImplementedException("ITERATE statement is not yet implemented (Phase 3)");
    }

    private Statement ParseReturnStatement()
    {
        throw new NotImplementedException("RETURN statement is not yet implemented (Phase 3)");
    }

    #endregion
}
