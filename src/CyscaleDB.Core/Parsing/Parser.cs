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
            statements.Add(ParseStatement());
            
            // Consume optional semicolon
            while (Check(TokenType.Semicolon))
            {
                Advance();
            }
        }

        return statements;
    }

    private Statement ParseStatement()
    {
        return _currentToken.Type switch
        {
            TokenType.SELECT => ParseSelectStatement(),
            TokenType.INSERT => ParseInsertStatement(),
            TokenType.UPDATE => ParseUpdateStatement(),
            TokenType.DELETE => ParseDeleteStatement(),
            TokenType.CREATE => ParseCreateStatement(),
            TokenType.DROP => ParseDropStatement(),
            TokenType.USE => ParseUseStatement(),
            TokenType.SHOW => ParseShowStatement(),
            TokenType.DESCRIBE => ParseDescribeStatement(),
            TokenType.BEGIN => ParseBeginStatement(),
            TokenType.COMMIT => ParseCommitStatement(),
            TokenType.ROLLBACK => ParseRollbackStatement(),
            TokenType.OPTIMIZE => ParseOptimizeStatement(),
            _ => throw Error($"Unexpected token at start of statement: {_currentToken.Value}")
        };
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

        // LIMIT clause
        if (Check(TokenType.LIMIT))
        {
            Advance();
            stmt.Limit = ParseInteger();

            // OFFSET clause
            if (Check(TokenType.OFFSET))
            {
                Advance();
                stmt.Offset = ParseInteger();
            }
        }

        return stmt;
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

        if (Check(TokenType.Identifier))
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
            var joinType = ParseJoinType();
            var right = ParseSimpleTableReference();

            Expression? condition = null;
            if (Match(TokenType.ON))
            {
                condition = ParseExpression();
            }

            left = new JoinTableReference
            {
                Left = left,
                Right = right,
                JoinType = joinType,
                Condition = condition
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
               Check(TokenType.FULL) || Check(TokenType.CROSS);
    }

    private JoinType ParseJoinType()
    {
        JoinType joinType = JoinType.Inner;

        if (Match(TokenType.INNER))
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

        // Check for UNIQUE keyword before INDEX
        bool isUnique = false;
        if (Match(TokenType.UNIQUE))
        {
            isUnique = true;
        }

        if (Check(TokenType.TABLE))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE TABLE");
            return ParseCreateTableStatement();
        }
        else if (Check(TokenType.DATABASE))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE DATABASE");
            return ParseCreateDatabaseStatement();
        }
        else if (Check(TokenType.INDEX))
        {
            return ParseCreateIndexStatement(isUnique);
        }
        else if (Check(TokenType.VIEW) || Check(TokenType.OR))
        {
            if (isUnique)
                throw Error("UNIQUE cannot be used with CREATE VIEW");
            return ParseCreateViewStatement();
        }

        throw Error($"Expected TABLE, DATABASE, INDEX, or VIEW after CREATE, got: {_currentToken.Value}");
    }

    private CreateIndexStatement ParseCreateIndexStatement(bool isUnique)
    {
        Expect(TokenType.INDEX);

        var stmt = new CreateIndexStatement();
        stmt.IsUnique = isUnique;

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

        // Optional USING clause for index type
        if (Match(TokenType.USING))
        {
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
        col.DataType = ParseDataType(out var length, out var precision, out var scale);
        col.Length = length;
        col.Precision = precision;
        col.Scale = scale;

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
        length = null;
        precision = null;
        scale = null;

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
            _ => throw Error($"Expected data type, got: {token.Value}")
        };

        // Parse length/precision/scale
        if (Match(TokenType.LeftParen))
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

            Expect(TokenType.RightParen);
        }

        return dataType;
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

        throw Error($"Expected TABLE, DATABASE, INDEX, or VIEW after DROP, got: {_currentToken.Value}");
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

        if (Check(TokenType.TABLES))
        {
            Advance();
            var stmt = new ShowTablesStatement();
            if (Match(TokenType.FROM))
            {
                stmt.DatabaseName = ExpectIdentifier();
            }
            return stmt;
        }
        else if (Check(TokenType.DATABASES))
        {
            Advance();
            return new ShowDatabasesStatement();
        }

        throw Error($"Expected TABLES or DATABASES after SHOW, got: {_currentToken.Value}");
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

        return ParsePrimaryExpression();
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

        // Identifier (column or function call)
        if (Check(TokenType.Identifier) || IsAggregateFunction())
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

    private FunctionCall ParseFunctionCall(string functionName)
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

        Expect(TokenType.RightParen);
        return func;
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
        if (!Check(TokenType.Identifier))
        {
            throw Error($"Expected identifier, got: {_currentToken.Value}");
        }

        var value = _currentToken.Value;
        Advance();
        return value;
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
        return type >= TokenType.SELECT && type <= TokenType.MAX;
    }

    private SqlSyntaxException Error(string message)
    {
        return new SqlSyntaxException(message, _currentToken.Position, _currentToken.Line, _currentToken.Column);
    }

    #endregion
}
