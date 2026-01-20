using CyscaleDB.Core.Parsing;

namespace CyscaleDB.Tests;

public class LexerTests
{
    [Fact]
    public void Tokenize_EmptyInput_ReturnsOnlyEof()
    {
        var lexer = new Lexer("");
        var tokens = lexer.Tokenize();
        
        Assert.Single(tokens);
        Assert.Equal(TokenType.EOF, tokens[0].Type);
    }

    [Fact]
    public void Tokenize_WhitespaceOnly_ReturnsOnlyEof()
    {
        var lexer = new Lexer("   \t\n\r  ");
        var tokens = lexer.Tokenize();
        
        Assert.Single(tokens);
        Assert.Equal(TokenType.EOF, tokens[0].Type);
    }

    [Theory]
    [InlineData("SELECT", TokenType.SELECT)]
    [InlineData("select", TokenType.SELECT)]
    [InlineData("INSERT", TokenType.INSERT)]
    [InlineData("UPDATE", TokenType.UPDATE)]
    [InlineData("DELETE", TokenType.DELETE)]
    [InlineData("CREATE", TokenType.CREATE)]
    [InlineData("DROP", TokenType.DROP)]
    [InlineData("TABLE", TokenType.TABLE)]
    [InlineData("FROM", TokenType.FROM)]
    [InlineData("WHERE", TokenType.WHERE)]
    [InlineData("AND", TokenType.AND)]
    [InlineData("OR", TokenType.OR)]
    [InlineData("NOT", TokenType.NOT)]
    [InlineData("PRIMARY", TokenType.PRIMARY)]
    [InlineData("KEY", TokenType.KEY)]
    [InlineData("VARCHAR", TokenType.VARCHAR)]
    [InlineData("INT", TokenType.INT)]
    [InlineData("INTEGER", TokenType.INTEGER)]
    [InlineData("BIGINT", TokenType.BIGINT)]
    [InlineData("BOOLEAN", TokenType.BOOLEAN)]
    public void Tokenize_Keywords_ReturnsCorrectType(string input, TokenType expected)
    {
        var lexer = new Lexer(input);
        var tokens = lexer.Tokenize();
        
        Assert.Equal(2, tokens.Count);
        Assert.Equal(expected, tokens[0].Type);
        Assert.Equal(TokenType.EOF, tokens[1].Type);
    }

    [Theory]
    [InlineData("123", TokenType.IntegerLiteral, "123")]
    [InlineData("0", TokenType.IntegerLiteral, "0")]
    [InlineData("999999", TokenType.IntegerLiteral, "999999")]
    public void Tokenize_IntegerLiterals_ReturnsCorrectValue(string input, TokenType expectedType, string expectedValue)
    {
        var lexer = new Lexer(input);
        var tokens = lexer.Tokenize();
        
        Assert.Equal(expectedType, tokens[0].Type);
        Assert.Equal(expectedValue, tokens[0].Value);
    }

    [Theory]
    [InlineData("3.14", TokenType.FloatLiteral, "3.14")]
    [InlineData("0.5", TokenType.FloatLiteral, "0.5")]
    public void Tokenize_FloatLiterals_ReturnsCorrectValue(string input, TokenType expectedType, string expectedValue)
    {
        var lexer = new Lexer(input);
        var tokens = lexer.Tokenize();
        
        Assert.Equal(expectedType, tokens[0].Type);
        Assert.Equal(expectedValue, tokens[0].Value);
    }

    [Theory]
    [InlineData("'hello'", "hello")]
    [InlineData("\"world\"", "world")]
    [InlineData("'it''s'", "it's")]
    [InlineData("''", "")]
    public void Tokenize_StringLiterals_ReturnsCorrectValue(string input, string expectedValue)
    {
        var lexer = new Lexer(input);
        var tokens = lexer.Tokenize();
        
        Assert.Equal(TokenType.StringLiteral, tokens[0].Type);
        Assert.Equal(expectedValue, tokens[0].Value);
    }

    [Theory]
    [InlineData("users", "users")]
    [InlineData("_private", "_private")]
    [InlineData("column1", "column1")]
    [InlineData("`select`", "select")]
    [InlineData("`table name`", "table name")]
    public void Tokenize_Identifiers_ReturnsCorrectValue(string input, string expectedValue)
    {
        var lexer = new Lexer(input);
        var tokens = lexer.Tokenize();
        
        Assert.Equal(TokenType.Identifier, tokens[0].Type);
        Assert.Equal(expectedValue, tokens[0].Value);
    }

    [Theory]
    [InlineData("=", TokenType.Equal)]
    [InlineData("!=", TokenType.NotEqual)]
    [InlineData("<>", TokenType.NotEqual)]
    [InlineData("<", TokenType.LessThan)]
    [InlineData("<=", TokenType.LessThanOrEqual)]
    [InlineData(">", TokenType.GreaterThan)]
    [InlineData(">=", TokenType.GreaterThanOrEqual)]
    [InlineData("+", TokenType.Plus)]
    [InlineData("-", TokenType.Minus)]
    [InlineData("*", TokenType.Asterisk)]
    [InlineData("/", TokenType.Slash)]
    [InlineData("%", TokenType.Percent)]
    [InlineData("(", TokenType.LeftParen)]
    [InlineData(")", TokenType.RightParen)]
    [InlineData(",", TokenType.Comma)]
    [InlineData(";", TokenType.Semicolon)]
    [InlineData(".", TokenType.Dot)]
    public void Tokenize_Operators_ReturnsCorrectType(string input, TokenType expected)
    {
        var lexer = new Lexer(input);
        var tokens = lexer.Tokenize();
        
        Assert.Equal(expected, tokens[0].Type);
    }

    [Fact]
    public void Tokenize_BooleanLiterals_ReturnsCorrectType()
    {
        var lexerTrue = new Lexer("TRUE");
        var tokensTrue = lexerTrue.Tokenize();
        Assert.Equal(TokenType.TRUE, tokensTrue[0].Type);
        
        var lexerFalse = new Lexer("false");
        var tokensFalse = lexerFalse.Tokenize();
        Assert.Equal(TokenType.FALSE, tokensFalse[0].Type);
    }

    [Fact]
    public void Tokenize_NullLiteral_ReturnsCorrectType()
    {
        var lexer = new Lexer("NULL");
        var tokens = lexer.Tokenize();
        
        Assert.Equal(TokenType.NULL, tokens[0].Type);
    }

    [Fact]
    public void Tokenize_SimpleSelect_ReturnsCorrectTokens()
    {
        var lexer = new Lexer("SELECT * FROM users");
        var tokens = lexer.Tokenize();
        
        Assert.Equal(5, tokens.Count);
        Assert.Equal(TokenType.SELECT, tokens[0].Type);
        Assert.Equal(TokenType.Asterisk, tokens[1].Type);
        Assert.Equal(TokenType.FROM, tokens[2].Type);
        Assert.Equal(TokenType.Identifier, tokens[3].Type);
        Assert.Equal("users", tokens[3].Value);
        Assert.Equal(TokenType.EOF, tokens[4].Type);
    }

    [Fact]
    public void Tokenize_SelectWithWhere_ReturnsCorrectTokens()
    {
        var lexer = new Lexer("SELECT id, name FROM users WHERE age >= 18");
        var tokens = lexer.Tokenize();
        
        var expectedTypes = new[]
        {
            TokenType.SELECT,
            TokenType.Identifier, // id
            TokenType.Comma,
            TokenType.Identifier, // name
            TokenType.FROM,
            TokenType.Identifier, // users
            TokenType.WHERE,
            TokenType.Identifier, // age
            TokenType.GreaterThanOrEqual,
            TokenType.IntegerLiteral, // 18
            TokenType.EOF
        };
        
        Assert.Equal(expectedTypes.Length, tokens.Count);
        for (int i = 0; i < expectedTypes.Length; i++)
        {
            Assert.Equal(expectedTypes[i], tokens[i].Type);
        }
    }

    [Fact]
    public void Tokenize_CreateTable_ReturnsCorrectTokens()
    {
        var lexer = new Lexer("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");
        var tokens = lexer.Tokenize();
        
        var expectedTypes = new[]
        {
            TokenType.CREATE,
            TokenType.TABLE,
            TokenType.Identifier, // users
            TokenType.LeftParen,
            TokenType.Identifier, // id
            TokenType.INT,
            TokenType.PRIMARY,
            TokenType.KEY,
            TokenType.Comma,
            TokenType.Identifier, // name
            TokenType.VARCHAR,
            TokenType.LeftParen,
            TokenType.IntegerLiteral, // 100
            TokenType.RightParen,
            TokenType.RightParen,
            TokenType.EOF
        };
        
        Assert.Equal(expectedTypes.Length, tokens.Count);
        for (int i = 0; i < expectedTypes.Length; i++)
        {
            Assert.Equal(expectedTypes[i], tokens[i].Type);
        }
    }

    [Fact]
    public void Tokenize_SingleLineComment_IgnoresComment()
    {
        var lexer = new Lexer("SELECT -- this is a comment\n* FROM users");
        var tokens = lexer.Tokenize();
        
        Assert.Equal(TokenType.SELECT, tokens[0].Type);
        Assert.Equal(TokenType.Asterisk, tokens[1].Type);
        Assert.Equal(TokenType.FROM, tokens[2].Type);
    }

    [Fact]
    public void Tokenize_HashComment_IgnoresComment()
    {
        var lexer = new Lexer("SELECT # comment\n* FROM users");
        var tokens = lexer.Tokenize();
        
        Assert.Equal(TokenType.SELECT, tokens[0].Type);
        Assert.Equal(TokenType.Asterisk, tokens[1].Type);
    }

    [Fact]
    public void Tokenize_MultiLineComment_IgnoresComment()
    {
        var lexer = new Lexer("SELECT /* this is\na multi-line\ncomment */ * FROM users");
        var tokens = lexer.Tokenize();
        
        Assert.Equal(TokenType.SELECT, tokens[0].Type);
        Assert.Equal(TokenType.Asterisk, tokens[1].Type);
        Assert.Equal(TokenType.FROM, tokens[2].Type);
    }

    [Fact]
    public void Tokenize_TracksLineAndColumn()
    {
        var lexer = new Lexer("SELECT\n  id\n  FROM");
        var tokens = lexer.Tokenize();
        
        Assert.Equal(1, tokens[0].Line); // SELECT
        Assert.Equal(1, tokens[0].Column);
        
        Assert.Equal(2, tokens[1].Line); // id
        Assert.Equal(3, tokens[1].Column);
        
        Assert.Equal(3, tokens[2].Line); // FROM
        Assert.Equal(3, tokens[2].Column);
    }

    [Fact]
    public void Tokenize_Insert_ReturnsCorrectTokens()
    {
        var lexer = new Lexer("INSERT INTO users (name, age) VALUES ('John', 30)");
        var tokens = lexer.Tokenize();
        
        var expectedTypes = new[]
        {
            TokenType.INSERT,
            TokenType.INTO,
            TokenType.Identifier, // users
            TokenType.LeftParen,
            TokenType.Identifier, // name
            TokenType.Comma,
            TokenType.Identifier, // age
            TokenType.RightParen,
            TokenType.VALUES,
            TokenType.LeftParen,
            TokenType.StringLiteral, // 'John'
            TokenType.Comma,
            TokenType.IntegerLiteral, // 30
            TokenType.RightParen,
            TokenType.EOF
        };
        
        Assert.Equal(expectedTypes.Length, tokens.Count);
        for (int i = 0; i < expectedTypes.Length; i++)
        {
            Assert.Equal(expectedTypes[i], tokens[i].Type);
        }
    }

    [Fact]
    public void Tokenize_JoinKeywords_ReturnsCorrectTokens()
    {
        var lexer = new Lexer("INNER JOIN LEFT OUTER JOIN RIGHT JOIN CROSS JOIN");
        var tokens = lexer.Tokenize();
        
        var expectedTypes = new[]
        {
            TokenType.INNER,
            TokenType.JOIN,
            TokenType.LEFT,
            TokenType.OUTER,
            TokenType.JOIN,
            TokenType.RIGHT,
            TokenType.JOIN,
            TokenType.CROSS,
            TokenType.JOIN,
            TokenType.EOF
        };
        
        Assert.Equal(expectedTypes.Length, tokens.Count);
        for (int i = 0; i < expectedTypes.Length; i++)
        {
            Assert.Equal(expectedTypes[i], tokens[i].Type);
        }
    }

    [Fact]
    public void Tokenize_AutoIncrement_ReturnsCorrectToken()
    {
        var lexer = new Lexer("AUTO_INCREMENT");
        var tokens = lexer.Tokenize();
        
        Assert.Equal(TokenType.AUTO_INCREMENT, tokens[0].Type);
    }

    [Fact]
    public void NextToken_CanBeCalledRepeatedly()
    {
        var lexer = new Lexer("SELECT * FROM");
        
        Assert.Equal(TokenType.SELECT, lexer.NextToken().Type);
        Assert.Equal(TokenType.Asterisk, lexer.NextToken().Type);
        Assert.Equal(TokenType.FROM, lexer.NextToken().Type);
        Assert.Equal(TokenType.EOF, lexer.NextToken().Type);
        Assert.Equal(TokenType.EOF, lexer.NextToken().Type); // Should keep returning EOF
    }

}
