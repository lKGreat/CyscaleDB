using CyscaleDB.Core.Common;
using CyscaleDB.Core.Parsing;
using CyscaleDB.Core.Parsing.Ast;

namespace CyscaleDB.Tests;

public class ParserTests
{
    #region CREATE TABLE Tests

    [Fact]
    public void Parse_SimpleCreateTable_ReturnsCorrectAst()
    {
        var parser = new Parser("CREATE TABLE users (id INT, name VARCHAR(100))");
        var stmt = parser.Parse() as CreateTableStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.TableName);
        Assert.Null(stmt.DatabaseName);
        Assert.False(stmt.IfNotExists);
        Assert.Equal(2, stmt.Columns.Count);
        
        Assert.Equal("id", stmt.Columns[0].Name);
        Assert.Equal(DataType.Int, stmt.Columns[0].DataType);
        
        Assert.Equal("name", stmt.Columns[1].Name);
        Assert.Equal(DataType.VarChar, stmt.Columns[1].DataType);
        Assert.Equal(100, stmt.Columns[1].Length);
    }

    [Fact]
    public void Parse_CreateTableIfNotExists_ReturnsCorrectAst()
    {
        var parser = new Parser("CREATE TABLE IF NOT EXISTS users (id INT)");
        var stmt = parser.Parse() as CreateTableStatement;
        
        Assert.NotNull(stmt);
        Assert.True(stmt.IfNotExists);
    }

    [Fact]
    public void Parse_CreateTableWithQualifiedName_ReturnsCorrectAst()
    {
        var parser = new Parser("CREATE TABLE mydb.users (id INT)");
        var stmt = parser.Parse() as CreateTableStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("mydb", stmt.DatabaseName);
        Assert.Equal("users", stmt.TableName);
    }

    [Fact]
    public void Parse_CreateTableWithConstraints_ReturnsCorrectAst()
    {
        var parser = new Parser(@"
            CREATE TABLE users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                email VARCHAR(255) NOT NULL UNIQUE,
                age INT DEFAULT 0
            )");
        var stmt = parser.Parse() as CreateTableStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal(3, stmt.Columns.Count);
        
        // id column
        Assert.True(stmt.Columns[0].IsPrimaryKey);
        Assert.True(stmt.Columns[0].IsAutoIncrement);
        Assert.False(stmt.Columns[0].IsNullable);
        
        // email column
        Assert.False(stmt.Columns[1].IsNullable);
        Assert.True(stmt.Columns[1].IsUnique);
        
        // age column with default
        Assert.NotNull(stmt.Columns[2].DefaultValue);
    }

    [Fact]
    public void Parse_CreateTableWithAllDataTypes_ReturnsCorrectAst()
    {
        var parser = new Parser(@"
            CREATE TABLE all_types (
                col_int INT,
                col_bigint BIGINT,
                col_smallint SMALLINT,
                col_tinyint TINYINT,
                col_varchar VARCHAR(50),
                col_char CHAR(10),
                col_text TEXT,
                col_bool BOOLEAN,
                col_datetime DATETIME,
                col_date DATE,
                col_time TIME,
                col_timestamp TIMESTAMP,
                col_float FLOAT,
                col_double DOUBLE,
                col_decimal DECIMAL(10, 2),
                col_blob BLOB
            )");
        var stmt = parser.Parse() as CreateTableStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal(16, stmt.Columns.Count);
        
        Assert.Equal(DataType.Int, stmt.Columns[0].DataType);
        Assert.Equal(DataType.BigInt, stmt.Columns[1].DataType);
        Assert.Equal(DataType.SmallInt, stmt.Columns[2].DataType);
        Assert.Equal(DataType.TinyInt, stmt.Columns[3].DataType);
        Assert.Equal(DataType.VarChar, stmt.Columns[4].DataType);
        Assert.Equal(50, stmt.Columns[4].Length);
        Assert.Equal(DataType.Char, stmt.Columns[5].DataType);
        Assert.Equal(10, stmt.Columns[5].Length);
        Assert.Equal(DataType.Text, stmt.Columns[6].DataType);
        Assert.Equal(DataType.Boolean, stmt.Columns[7].DataType);
        Assert.Equal(DataType.DateTime, stmt.Columns[8].DataType);
        Assert.Equal(DataType.Date, stmt.Columns[9].DataType);
        Assert.Equal(DataType.Time, stmt.Columns[10].DataType);
        Assert.Equal(DataType.Timestamp, stmt.Columns[11].DataType);
        Assert.Equal(DataType.Float, stmt.Columns[12].DataType);
        Assert.Equal(DataType.Double, stmt.Columns[13].DataType);
        Assert.Equal(DataType.Decimal, stmt.Columns[14].DataType);
        Assert.Equal(10, stmt.Columns[14].Precision);
        Assert.Equal(2, stmt.Columns[14].Scale);
        Assert.Equal(DataType.Blob, stmt.Columns[15].DataType);
    }

    #endregion

    #region DROP TABLE Tests

    [Fact]
    public void Parse_SimpleDropTable_ReturnsCorrectAst()
    {
        var parser = new Parser("DROP TABLE users");
        var stmt = parser.Parse() as DropTableStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.TableName);
        Assert.Null(stmt.DatabaseName);
        Assert.False(stmt.IfExists);
    }

    [Fact]
    public void Parse_DropTableIfExists_ReturnsCorrectAst()
    {
        var parser = new Parser("DROP TABLE IF EXISTS users");
        var stmt = parser.Parse() as DropTableStatement;
        
        Assert.NotNull(stmt);
        Assert.True(stmt.IfExists);
    }

    [Fact]
    public void Parse_DropTableWithQualifiedName_ReturnsCorrectAst()
    {
        var parser = new Parser("DROP TABLE mydb.users");
        var stmt = parser.Parse() as DropTableStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("mydb", stmt.DatabaseName);
        Assert.Equal("users", stmt.TableName);
    }

    #endregion

    #region INSERT Tests

    [Fact]
    public void Parse_SimpleInsert_ReturnsCorrectAst()
    {
        var parser = new Parser("INSERT INTO users VALUES (1, 'John')");
        var stmt = parser.Parse() as InsertStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.TableName);
        Assert.Empty(stmt.Columns);
        Assert.Single(stmt.ValuesList);
        Assert.Equal(2, stmt.ValuesList[0].Count);
    }

    [Fact]
    public void Parse_InsertWithColumns_ReturnsCorrectAst()
    {
        var parser = new Parser("INSERT INTO users (id, name) VALUES (1, 'John')");
        var stmt = parser.Parse() as InsertStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.Columns);
        Assert.Equal(2, stmt.Columns.Count);
        Assert.Equal("id", stmt.Columns[0]);
        Assert.Equal("name", stmt.Columns[1]);
    }

    [Fact]
    public void Parse_InsertMultipleRows_ReturnsCorrectAst()
    {
        var parser = new Parser("INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Bob')");
        var stmt = parser.Parse() as InsertStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal(3, stmt.ValuesList.Count);
    }

    [Fact]
    public void Parse_InsertWithQualifiedTable_ReturnsCorrectAst()
    {
        var parser = new Parser("INSERT INTO mydb.users (id) VALUES (1)");
        var stmt = parser.Parse() as InsertStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("mydb", stmt.DatabaseName);
        Assert.Equal("users", stmt.TableName);
    }

    [Fact]
    public void Parse_InsertWithExpressions_ReturnsCorrectAst()
    {
        var parser = new Parser("INSERT INTO orders (total) VALUES (100 + 50)");
        var stmt = parser.Parse() as InsertStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.ValuesList[0][0] as BinaryExpression;
        Assert.NotNull(expr);
        Assert.Equal(BinaryOperator.Add, expr.Operator);
    }

    #endregion

    #region SELECT Tests

    [Fact]
    public void Parse_SimpleSelect_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.Single(stmt.Columns);
        Assert.True(stmt.Columns[0].IsWildcard);
        Assert.NotNull(stmt.From);
        
        var table = stmt.From as SimpleTableReference;
        Assert.NotNull(table);
        Assert.Equal("users", table.TableName);
    }

    [Fact]
    public void Parse_SelectWithColumns_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT id, name, email FROM users");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal(3, stmt.Columns.Count);
        
        var col1 = stmt.Columns[0].Expression as ColumnReference;
        Assert.NotNull(col1);
        Assert.Equal("id", col1.ColumnName);
    }

    [Fact]
    public void Parse_SelectDistinct_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT DISTINCT name FROM users");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.True(stmt.IsDistinct);
    }

    [Fact]
    public void Parse_SelectWithAlias_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT id AS user_id, name username FROM users u");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("user_id", stmt.Columns[0].Alias);
        Assert.Equal("username", stmt.Columns[1].Alias);
        
        var table = stmt.From as SimpleTableReference;
        Assert.NotNull(table);
        Assert.Equal("u", table.Alias);
    }

    [Fact]
    public void Parse_SelectWithWhere_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users WHERE age >= 18");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.Where);
        
        var where = stmt.Where as BinaryExpression;
        Assert.NotNull(where);
        Assert.Equal(BinaryOperator.GreaterThanOrEqual, where.Operator);
    }

    [Fact]
    public void Parse_SelectWithComplexWhere_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users WHERE age >= 18 AND status = 'active' OR role = 'admin'");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.Where);
        
        // OR has lower precedence than AND
        var orExpr = stmt.Where as BinaryExpression;
        Assert.NotNull(orExpr);
        Assert.Equal(BinaryOperator.Or, orExpr.Operator);
        
        var andExpr = orExpr.Left as BinaryExpression;
        Assert.NotNull(andExpr);
        Assert.Equal(BinaryOperator.And, andExpr.Operator);
    }

    [Fact]
    public void Parse_SelectWithInnerJoin_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.From);
        
        var join = stmt.From as JoinTableReference;
        Assert.NotNull(join);
        Assert.Equal(JoinType.Inner, join.JoinType);
        Assert.NotNull(join.Condition);
    }

    [Fact]
    public void Parse_SelectWithLeftJoin_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.From);
        
        var join = stmt.From as JoinTableReference;
        Assert.NotNull(join);
        Assert.Equal(JoinType.Left, join.JoinType);
    }

    [Fact]
    public void Parse_SelectWithOrderBy_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users ORDER BY name ASC, created_at DESC");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.OrderBy);
        Assert.Equal(2, stmt.OrderBy.Count);
        Assert.False(stmt.OrderBy[0].Descending);
        Assert.True(stmt.OrderBy[1].Descending);
    }

    [Fact]
    public void Parse_SelectWithLimit_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users LIMIT 10");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal(10, stmt.Limit);
        Assert.Null(stmt.Offset);
    }

    [Fact]
    public void Parse_SelectWithLimitOffset_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users LIMIT 10 OFFSET 20");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal(10, stmt.Limit);
        Assert.Equal(20, stmt.Offset);
    }

    [Fact]
    public void Parse_SelectWithGroupBy_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT department, COUNT(*) FROM employees GROUP BY department");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.GroupBy);
        Assert.Single(stmt.GroupBy);
    }

    [Fact]
    public void Parse_SelectWithGroupByHaving_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT department, COUNT(*) as cnt FROM employees GROUP BY department HAVING cnt > 5");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.GroupBy);
        Assert.NotNull(stmt.Having);
    }

    [Fact]
    public void Parse_SelectWithQualifiedColumn_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT users.id FROM users");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        
        var col1 = stmt.Columns[0].Expression as ColumnReference;
        Assert.NotNull(col1);
        Assert.Equal("users", col1.TableName);
        Assert.Equal("id", col1.ColumnName);
    }

    [Fact]
    public void Parse_SelectWithTableWildcard_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT users.* FROM users");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.True(stmt.Columns[0].IsWildcard);
        Assert.Equal("users", stmt.Columns[0].TableQualifier);
    }

    #endregion

    #region UPDATE Tests

    [Fact]
    public void Parse_SimpleUpdate_ReturnsCorrectAst()
    {
        var parser = new Parser("UPDATE users SET name = 'John'");
        var stmt = parser.Parse() as UpdateStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.TableName);
        Assert.Single(stmt.SetClauses);
        Assert.Equal("name", stmt.SetClauses[0].ColumnName);
        Assert.Null(stmt.Where);
    }

    [Fact]
    public void Parse_UpdateWithMultipleAssignments_ReturnsCorrectAst()
    {
        var parser = new Parser("UPDATE users SET name = 'John', age = 30, active = TRUE");
        var stmt = parser.Parse() as UpdateStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal(3, stmt.SetClauses.Count);
    }

    [Fact]
    public void Parse_UpdateWithWhere_ReturnsCorrectAst()
    {
        var parser = new Parser("UPDATE users SET name = 'John' WHERE id = 1");
        var stmt = parser.Parse() as UpdateStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.Where);
    }

    #endregion

    #region DELETE Tests

    [Fact]
    public void Parse_SimpleDelete_ReturnsCorrectAst()
    {
        var parser = new Parser("DELETE FROM users");
        var stmt = parser.Parse() as DeleteStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal("users", stmt.TableName);
        Assert.Null(stmt.Where);
    }

    [Fact]
    public void Parse_DeleteWithWhere_ReturnsCorrectAst()
    {
        var parser = new Parser("DELETE FROM users WHERE id = 1");
        var stmt = parser.Parse() as DeleteStatement;
        
        Assert.NotNull(stmt);
        Assert.NotNull(stmt.Where);
    }

    #endregion

    #region Expression Tests

    [Fact]
    public void Parse_ArithmeticExpression_RespectsOperatorPrecedence()
    {
        var parser = new Parser("SELECT 1 + 2 * 3 FROM dual");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Columns[0].Expression as BinaryExpression;
        Assert.NotNull(expr);
        Assert.Equal(BinaryOperator.Add, expr.Operator);
        
        // Right side should be 2 * 3
        var right = expr.Right as BinaryExpression;
        Assert.NotNull(right);
        Assert.Equal(BinaryOperator.Multiply, right.Operator);
    }

    [Fact]
    public void Parse_ParenthesizedExpression_OverridesPrecedence()
    {
        var parser = new Parser("SELECT (1 + 2) * 3 FROM dual");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Columns[0].Expression as BinaryExpression;
        Assert.NotNull(expr);
        Assert.Equal(BinaryOperator.Multiply, expr.Operator);
        
        // Left side should be 1 + 2
        var left = expr.Left as BinaryExpression;
        Assert.NotNull(left);
        Assert.Equal(BinaryOperator.Add, left.Operator);
    }

    [Fact]
    public void Parse_UnaryNegation_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT -price FROM products");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Columns[0].Expression as UnaryExpression;
        Assert.NotNull(expr);
        Assert.Equal(UnaryOperator.Negate, expr.Operator);
    }

    [Fact]
    public void Parse_NotExpression_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users WHERE NOT active");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Where as UnaryExpression;
        Assert.NotNull(expr);
        Assert.Equal(UnaryOperator.Not, expr.Operator);
    }

    [Fact]
    public void Parse_IsNullExpression_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users WHERE email IS NULL");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Where as IsNullExpression;
        Assert.NotNull(expr);
        Assert.False(expr.IsNot);
    }

    [Fact]
    public void Parse_IsNotNullExpression_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users WHERE email IS NOT NULL");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Where as IsNullExpression;
        Assert.NotNull(expr);
        Assert.True(expr.IsNot);
    }

    [Fact]
    public void Parse_BetweenExpression_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM products WHERE price BETWEEN 10 AND 100");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Where as BetweenExpression;
        Assert.NotNull(expr);
        Assert.False(expr.IsNot);
    }

    [Fact]
    public void Parse_NotBetweenExpression_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM products WHERE price NOT BETWEEN 10 AND 100");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Where as BetweenExpression;
        Assert.NotNull(expr);
        Assert.True(expr.IsNot);
    }

    [Fact]
    public void Parse_InExpression_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users WHERE status IN ('active', 'pending')");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Where as InExpression;
        Assert.NotNull(expr);
        Assert.False(expr.IsNot);
        Assert.NotNull(expr.Values);
        Assert.Equal(2, expr.Values.Count);
    }

    [Fact]
    public void Parse_NotInExpression_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users WHERE status NOT IN ('deleted', 'banned')");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var expr = stmt.Where as InExpression;
        Assert.NotNull(expr);
        Assert.True(expr.IsNot);
    }

    [Fact]
    public void Parse_LikeExpression_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT * FROM users WHERE name LIKE 'John%'");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        // Check that WHERE clause is not null and contains a binary expression with LIKE operator
        Assert.NotNull(stmt.Where);
        var expr = stmt.Where as BinaryExpression;
        Assert.NotNull(expr);
        Assert.Equal(BinaryOperator.Like, expr.Operator);
    }

    [Fact]
    public void Parse_FunctionCall_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT COUNT(*), SUM(price), AVG(quantity) FROM orders");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        Assert.Equal(3, stmt.Columns.Count);
        
        var count = stmt.Columns[0].Expression as FunctionCall;
        Assert.NotNull(count);
        Assert.Equal("COUNT", count.FunctionName);
        
        var sum = stmt.Columns[1].Expression as FunctionCall;
        Assert.NotNull(sum);
        Assert.Equal("SUM", sum.FunctionName);
    }

    [Fact]
    public void Parse_FunctionWithDistinct_ReturnsCorrectAst()
    {
        var parser = new Parser("SELECT COUNT(DISTINCT category) FROM products");
        var stmt = parser.Parse() as SelectStatement;
        
        Assert.NotNull(stmt);
        var func = stmt.Columns[0].Expression as FunctionCall;
        Assert.NotNull(func);
        Assert.True(func.IsDistinct);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public void Parse_InvalidSyntax_ThrowsSqlSyntaxException()
    {
        var parser = new Parser("SELECTT * FROM users");
        
        Assert.Throws<SqlSyntaxException>(() => parser.Parse());
    }

    [Fact]
    public void Parse_MissingTableName_ThrowsSqlSyntaxException()
    {
        var parser = new Parser("SELECT * FROM");
        
        Assert.Throws<SqlSyntaxException>(() => parser.Parse());
    }

    [Fact]
    public void Parse_MissingParenthesis_ThrowsSqlSyntaxException()
    {
        var parser = new Parser("CREATE TABLE users (id INT");
        
        Assert.Throws<SqlSyntaxException>(() => parser.Parse());
    }

    [Fact]
    public void Parse_InvalidDataType_ThrowsSqlSyntaxException()
    {
        var parser = new Parser("CREATE TABLE users (id INVALID_TYPE)");
        
        Assert.Throws<SqlSyntaxException>(() => parser.Parse());
    }

    #endregion

    #region Statement With Semicolon Tests

    [Fact]
    public void Parse_StatementWithTrailingSemicolon_Works()
    {
        var parser = new Parser("SELECT * FROM users;");
        var stmt = parser.Parse();
        
        Assert.IsType<SelectStatement>(stmt);
    }

    #endregion
}
