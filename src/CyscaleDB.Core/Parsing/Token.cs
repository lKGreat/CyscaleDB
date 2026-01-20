namespace CyscaleDB.Core.Parsing;

/// <summary>
/// Represents a token produced by the lexer.
/// </summary>
public readonly struct Token
{
    /// <summary>
    /// The type of this token.
    /// </summary>
    public TokenType Type { get; }

    /// <summary>
    /// The text value of this token.
    /// </summary>
    public string Value { get; }

    /// <summary>
    /// The position (character offset) in the input where this token starts.
    /// </summary>
    public int Position { get; }

    /// <summary>
    /// The line number (1-based) where this token appears.
    /// </summary>
    public int Line { get; }

    /// <summary>
    /// The column number (1-based) where this token appears.
    /// </summary>
    public int Column { get; }

    public Token(TokenType type, string value, int position, int line, int column)
    {
        Type = type;
        Value = value;
        Position = position;
        Line = line;
        Column = column;
    }

    public override string ToString()
    {
        return $"{Type}({Value}) at {Line}:{Column}";
    }

    /// <summary>
    /// Checks if this token is a keyword.
    /// </summary>
    public bool IsKeyword => Type >= TokenType.SELECT && Type <= TokenType.ROLLBACK;
}

/// <summary>
/// Token types recognized by the lexer.
/// </summary>
public enum TokenType
{
    // Special tokens
    EOF = 0,
    Invalid,

    // Literals
    Identifier,
    IntegerLiteral,
    FloatLiteral,
    StringLiteral,
    
    // Operators and punctuation
    Comma,              // ,
    Semicolon,          // ;
    LeftParen,          // (
    RightParen,         // )
    Asterisk,           // *
    Plus,               // +
    Minus,              // -
    Slash,              // /
    Percent,            // %
    Equal,              // =
    NotEqual,           // != or <>
    LessThan,           // <
    LessThanOrEqual,    // <=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    Dot,                // .

    // SQL Keywords (starting from 100 for easy identification)
    SELECT = 100,
    FROM,
    WHERE,
    INSERT,
    INTO,
    VALUES,
    UPDATE,
    SET,
    DELETE,
    CREATE,
    DROP,
    TABLE,
    DATABASE,
    INDEX,
    PRIMARY,
    KEY,
    FOREIGN,
    REFERENCES,
    NOT,
    NULL,
    DEFAULT,
    AUTO_INCREMENT,
    UNIQUE,
    AND,
    OR,
    IN,
    LIKE,
    BETWEEN,
    IS,
    AS,
    ON,
    JOIN,
    INNER,
    LEFT,
    RIGHT,
    OUTER,
    FULL,
    CROSS,
    ORDER,
    BY,
    ASC,
    DESC,
    LIMIT,
    OFFSET,
    GROUP,
    HAVING,
    DISTINCT,
    ALL,
    EXISTS,
    TRUE,
    FALSE,
    
    // Transaction keywords
    BEGIN,
    COMMIT,
    ROLLBACK,

    // Data type keywords
    INT,
    INTEGER,
    BIGINT,
    SMALLINT,
    TINYINT,
    VARCHAR,
    CHAR,
    TEXT,
    BOOLEAN,
    BOOL,
    DATETIME,
    DATE,
    TIME,
    TIMESTAMP,
    FLOAT,
    DOUBLE,
    DECIMAL,
    BLOB,

    // Additional keywords
    SHOW,
    TABLES,
    DATABASES,
    USE,
    DESCRIBE,
    EXPLAIN,
    IF,
    CONSTRAINT,
    CHECK,
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX,

    // Index keywords
    BTREE,
    HASH,
    USING,

    // View keywords
    VIEW,
    REPLACE,

    // Optimization keywords
    OPTIMIZE,

    // SET statement keywords
    NAMES,
    GLOBAL,
    SESSION,
    VARIABLES,
    STATUS,
    COLUMNS,
    WARNINGS,
    ERRORS,
    START,
    TRANSACTION,
    KILL,
    CHARSET,
    COLLATION,

    // System variable prefix
    AtAt,               // @@
}

/// <summary>
/// Keyword lookup helper.
/// </summary>
public static class Keywords
{
    private static readonly Dictionary<string, TokenType> _keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        // SQL keywords
        ["SELECT"] = TokenType.SELECT,
        ["FROM"] = TokenType.FROM,
        ["WHERE"] = TokenType.WHERE,
        ["INSERT"] = TokenType.INSERT,
        ["INTO"] = TokenType.INTO,
        ["VALUES"] = TokenType.VALUES,
        ["UPDATE"] = TokenType.UPDATE,
        ["SET"] = TokenType.SET,
        ["DELETE"] = TokenType.DELETE,
        ["CREATE"] = TokenType.CREATE,
        ["DROP"] = TokenType.DROP,
        ["TABLE"] = TokenType.TABLE,
        ["DATABASE"] = TokenType.DATABASE,
        ["INDEX"] = TokenType.INDEX,
        ["PRIMARY"] = TokenType.PRIMARY,
        ["KEY"] = TokenType.KEY,
        ["FOREIGN"] = TokenType.FOREIGN,
        ["REFERENCES"] = TokenType.REFERENCES,
        ["NOT"] = TokenType.NOT,
        ["NULL"] = TokenType.NULL,
        ["DEFAULT"] = TokenType.DEFAULT,
        ["AUTO_INCREMENT"] = TokenType.AUTO_INCREMENT,
        ["UNIQUE"] = TokenType.UNIQUE,
        ["AND"] = TokenType.AND,
        ["OR"] = TokenType.OR,
        ["IN"] = TokenType.IN,
        ["LIKE"] = TokenType.LIKE,
        ["BETWEEN"] = TokenType.BETWEEN,
        ["IS"] = TokenType.IS,
        ["AS"] = TokenType.AS,
        ["ON"] = TokenType.ON,
        ["JOIN"] = TokenType.JOIN,
        ["INNER"] = TokenType.INNER,
        ["LEFT"] = TokenType.LEFT,
        ["RIGHT"] = TokenType.RIGHT,
        ["OUTER"] = TokenType.OUTER,
        ["FULL"] = TokenType.FULL,
        ["CROSS"] = TokenType.CROSS,
        ["ORDER"] = TokenType.ORDER,
        ["BY"] = TokenType.BY,
        ["ASC"] = TokenType.ASC,
        ["DESC"] = TokenType.DESC,
        ["LIMIT"] = TokenType.LIMIT,
        ["OFFSET"] = TokenType.OFFSET,
        ["GROUP"] = TokenType.GROUP,
        ["HAVING"] = TokenType.HAVING,
        ["DISTINCT"] = TokenType.DISTINCT,
        ["ALL"] = TokenType.ALL,
        ["EXISTS"] = TokenType.EXISTS,
        ["TRUE"] = TokenType.TRUE,
        ["FALSE"] = TokenType.FALSE,
        
        // Transaction
        ["BEGIN"] = TokenType.BEGIN,
        ["COMMIT"] = TokenType.COMMIT,
        ["ROLLBACK"] = TokenType.ROLLBACK,

        // Data types
        ["INT"] = TokenType.INT,
        ["INTEGER"] = TokenType.INTEGER,
        ["BIGINT"] = TokenType.BIGINT,
        ["SMALLINT"] = TokenType.SMALLINT,
        ["TINYINT"] = TokenType.TINYINT,
        ["VARCHAR"] = TokenType.VARCHAR,
        ["CHAR"] = TokenType.CHAR,
        ["TEXT"] = TokenType.TEXT,
        ["BOOLEAN"] = TokenType.BOOLEAN,
        ["BOOL"] = TokenType.BOOL,
        ["DATETIME"] = TokenType.DATETIME,
        ["DATE"] = TokenType.DATE,
        ["TIME"] = TokenType.TIME,
        ["TIMESTAMP"] = TokenType.TIMESTAMP,
        ["FLOAT"] = TokenType.FLOAT,
        ["DOUBLE"] = TokenType.DOUBLE,
        ["DECIMAL"] = TokenType.DECIMAL,
        ["BLOB"] = TokenType.BLOB,

        // Additional
        ["SHOW"] = TokenType.SHOW,
        ["TABLES"] = TokenType.TABLES,
        ["DATABASES"] = TokenType.DATABASES,
        ["USE"] = TokenType.USE,
        ["DESCRIBE"] = TokenType.DESCRIBE,
        ["EXPLAIN"] = TokenType.EXPLAIN,
        ["IF"] = TokenType.IF,
        ["CONSTRAINT"] = TokenType.CONSTRAINT,
        ["CHECK"] = TokenType.CHECK,
        ["COUNT"] = TokenType.COUNT,
        ["SUM"] = TokenType.SUM,
        ["AVG"] = TokenType.AVG,
        ["MIN"] = TokenType.MIN,
        ["MAX"] = TokenType.MAX,

        // Index keywords
        ["BTREE"] = TokenType.BTREE,
        ["HASH"] = TokenType.HASH,
        ["USING"] = TokenType.USING,

        // View keywords
        ["VIEW"] = TokenType.VIEW,
        ["REPLACE"] = TokenType.REPLACE,
        // Note: OR is already defined for the OR logical operator

        // Optimization keywords
        ["OPTIMIZE"] = TokenType.OPTIMIZE,

        // SET statement keywords
        ["NAMES"] = TokenType.NAMES,
        ["GLOBAL"] = TokenType.GLOBAL,
        ["SESSION"] = TokenType.SESSION,
        ["VARIABLES"] = TokenType.VARIABLES,
        ["STATUS"] = TokenType.STATUS,
        ["COLUMNS"] = TokenType.COLUMNS,
        ["WARNINGS"] = TokenType.WARNINGS,
        ["ERRORS"] = TokenType.ERRORS,
        ["START"] = TokenType.START,
        ["TRANSACTION"] = TokenType.TRANSACTION,
        ["KILL"] = TokenType.KILL,
        ["CHARSET"] = TokenType.CHARSET,
        ["COLLATION"] = TokenType.COLLATION,
    };

    /// <summary>
    /// Tries to get the keyword token type for the given identifier.
    /// </summary>
    public static bool TryGetKeyword(string identifier, out TokenType tokenType)
    {
        return _keywords.TryGetValue(identifier, out tokenType);
    }

    /// <summary>
    /// Gets the token type for an identifier, returning Identifier if not a keyword.
    /// </summary>
    public static TokenType GetTokenType(string identifier)
    {
        return _keywords.TryGetValue(identifier, out var type) ? type : TokenType.Identifier;
    }
}
