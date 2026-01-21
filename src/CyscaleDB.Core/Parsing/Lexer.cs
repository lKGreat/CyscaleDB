using CyscaleDB.Core.Common;

namespace CyscaleDB.Core.Parsing;

/// <summary>
/// SQL lexer that tokenizes input SQL strings.
/// </summary>
public sealed class Lexer
{
    private readonly string _input;
    private int _position;
    private int _line;
    private int _column;
    private int _tokenStart;
    private int _tokenLine;
    private int _tokenColumn;

    /// <summary>
    /// Creates a new lexer for the given input string.
    /// </summary>
    public Lexer(string input)
    {
        _input = input ?? string.Empty;
        _position = 0;
        _line = 1;
        _column = 1;
    }

    /// <summary>
    /// Gets the next token from the input.
    /// </summary>
    public Token NextToken()
    {
        SkipWhitespaceAndComments();

        if (IsAtEnd())
        {
            return MakeToken(TokenType.EOF, string.Empty);
        }

        _tokenStart = _position;
        _tokenLine = _line;
        _tokenColumn = _column;

        char c = Advance();

        // Identifiers and keywords
        if (IsIdentifierStart(c))
        {
            return ScanIdentifier();
        }

        // Numbers
        if (char.IsDigit(c))
        {
            return ScanNumber();
        }

        // Strings
        if (c == '\'' || c == '"')
        {
            return ScanString(c);
        }

        // Operators and punctuation
        return c switch
        {
            ',' => MakeToken(TokenType.Comma, ","),
            ';' => MakeToken(TokenType.Semicolon, ";"),
            '(' => MakeToken(TokenType.LeftParen, "("),
            ')' => MakeToken(TokenType.RightParen, ")"),
            '*' => MakeToken(TokenType.Asterisk, "*"),
            '+' => MakeToken(TokenType.Plus, "+"),
            '-' => ScanMinus(),
            '/' => MakeToken(TokenType.Slash, "/"),
            '%' => MakeToken(TokenType.Percent, "%"),
            '.' => MakeToken(TokenType.Dot, "."),
            '=' => MakeToken(TokenType.Equal, "="),
            '<' => ScanLessThan(),
            '>' => ScanGreaterThan(),
            '!' => ScanExclamation(),
            '`' => ScanBacktickIdentifier(),
            '@' => ScanAtSign(),
            _ => MakeToken(TokenType.Invalid, c.ToString())
        };
    }

    private Token ScanAtSign()
    {
        // Check for @@ (system variable)
        if (Match('@'))
        {
            return MakeToken(TokenType.AtAt, "@@");
        }
        // Single @ is invalid for now (user variables not supported)
        return MakeToken(TokenType.Invalid, "@");
    }

    private Token ScanMinus()
    {
        // Check for -> or ->>
        if (Match('>'))
        {
            if (Match('>'))
            {
                return MakeToken(TokenType.DoubleArrow, "->>");
            }
            return MakeToken(TokenType.Arrow, "->");
        }
        return MakeToken(TokenType.Minus, "-");
    }

    /// <summary>
    /// Tokenizes the entire input and returns all tokens.
    /// </summary>
    public List<Token> Tokenize()
    {
        var tokens = new List<Token>();
        Token token;
        do
        {
            token = NextToken();
            tokens.Add(token);
        } while (token.Type != TokenType.EOF);

        return tokens;
    }

    /// <summary>
    /// Peeks at the next token without consuming it.
    /// </summary>
    public Token Peek()
    {
        var savedPosition = _position;
        var savedLine = _line;
        var savedColumn = _column;

        var token = NextToken();

        _position = savedPosition;
        _line = savedLine;
        _column = savedColumn;

        return token;
    }

    private Token ScanIdentifier()
    {
        while (!IsAtEnd() && IsIdentifierChar(Current()))
        {
            Advance();
        }

        var value = _input[_tokenStart.._position];
        var tokenType = Keywords.GetTokenType(value);
        return MakeToken(tokenType, value);
    }

    private Token ScanNumber()
    {
        while (!IsAtEnd() && char.IsDigit(Current()))
        {
            Advance();
        }

        // Look for decimal part
        if (!IsAtEnd() && Current() == '.' && LookAhead(1) is char next && char.IsDigit(next))
        {
            Advance(); // consume '.'
            while (!IsAtEnd() && char.IsDigit(Current()))
            {
                Advance();
            }
            return MakeToken(TokenType.FloatLiteral, _input[_tokenStart.._position]);
        }

        return MakeToken(TokenType.IntegerLiteral, _input[_tokenStart.._position]);
    }

    private Token ScanString(char quote)
    {
        var sb = new System.Text.StringBuilder();

        while (!IsAtEnd())
        {
            char c = Current();

            if (c == quote)
            {
                // Check for escaped quote (doubled)
                if (LookAhead(1) == quote)
                {
                    sb.Append(quote);
                    Advance();
                    Advance();
                    continue;
                }
                break;
            }

            if (c == '\\')
            {
                Advance();
                if (!IsAtEnd())
                {
                    char escaped = Current();
                    sb.Append(escaped switch
                    {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '\\' => '\\',
                        '\'' => '\'',
                        '"' => '"',
                        '0' => '\0',
                        _ => escaped
                    });
                    Advance();
                }
                continue;
            }

            sb.Append(c);
            Advance();
        }

        if (IsAtEnd())
        {
            throw new SqlSyntaxException($"Unterminated string literal", _tokenStart, _tokenLine, _tokenColumn);
        }

        Advance(); // consume closing quote
        return MakeToken(TokenType.StringLiteral, sb.ToString());
    }

    private Token ScanBacktickIdentifier()
    {
        var start = _position; // after the opening backtick

        while (!IsAtEnd() && Current() != '`')
        {
            Advance();
        }

        if (IsAtEnd())
        {
            throw new SqlSyntaxException("Unterminated backtick identifier", _tokenStart, _tokenLine, _tokenColumn);
        }

        var value = _input[start.._position];
        Advance(); // consume closing backtick
        return MakeToken(TokenType.Identifier, value);
    }

    private Token ScanLessThan()
    {
        if (Match('='))
            return MakeToken(TokenType.LessThanOrEqual, "<=");
        if (Match('>'))
            return MakeToken(TokenType.NotEqual, "<>");
        return MakeToken(TokenType.LessThan, "<");
    }

    private Token ScanGreaterThan()
    {
        if (Match('='))
            return MakeToken(TokenType.GreaterThanOrEqual, ">=");
        return MakeToken(TokenType.GreaterThan, ">");
    }

    private Token ScanExclamation()
    {
        if (Match('='))
            return MakeToken(TokenType.NotEqual, "!=");
        return MakeToken(TokenType.Invalid, "!");
    }

    private void SkipWhitespaceAndComments()
    {
        while (!IsAtEnd())
        {
            char c = Current();

            // Whitespace
            if (char.IsWhiteSpace(c))
            {
                Advance();
                continue;
            }

            // Single-line comment: -- or #
            if (c == '-' && LookAhead(1) == '-')
            {
                Advance();
                Advance();
                while (!IsAtEnd() && Current() != '\n')
                {
                    Advance();
                }
                continue;
            }

            if (c == '#')
            {
                while (!IsAtEnd() && Current() != '\n')
                {
                    Advance();
                }
                continue;
            }

            // Multi-line comment: /* ... */
            if (c == '/' && LookAhead(1) == '*')
            {
                Advance();
                Advance();
                while (!IsAtEnd())
                {
                    if (Current() == '*' && LookAhead(1) == '/')
                    {
                        Advance();
                        Advance();
                        break;
                    }
                    Advance();
                }
                continue;
            }

            break;
        }
    }

    private char Current() => _position < _input.Length ? _input[_position] : '\0';

    private char? LookAhead(int offset)
    {
        var pos = _position + offset;
        return pos < _input.Length ? _input[pos] : null;
    }

    private char Advance()
    {
        char c = Current();
        _position++;

        if (c == '\n')
        {
            _line++;
            _column = 1;
        }
        else
        {
            _column++;
        }

        return c;
    }

    private bool Match(char expected)
    {
        if (IsAtEnd() || Current() != expected)
            return false;

        Advance();
        return true;
    }

    private bool IsAtEnd() => _position >= _input.Length;

    private static bool IsIdentifierStart(char c) =>
        char.IsLetter(c) || c == '_';

    private static bool IsIdentifierChar(char c) =>
        char.IsLetterOrDigit(c) || c == '_';

    private Token MakeToken(TokenType type, string value)
    {
        return new Token(type, value, _tokenStart, _tokenLine, _tokenColumn);
    }
}
