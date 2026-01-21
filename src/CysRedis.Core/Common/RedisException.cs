namespace CysRedis.Core.Common;

/// <summary>
/// Base exception for Redis errors.
/// </summary>
public class RedisException : Exception
{
    /// <summary>
    /// Error prefix for RESP protocol.
    /// </summary>
    public string ErrorPrefix { get; }

    public RedisException(string message, string errorPrefix = "ERR")
        : base(message)
    {
        ErrorPrefix = errorPrefix;
    }

    public RedisException(string message, Exception innerException, string errorPrefix = "ERR")
        : base(message, innerException)
    {
        ErrorPrefix = errorPrefix;
    }

    /// <summary>
    /// Gets the full error message for RESP protocol.
    /// </summary>
    public string GetRespError() => $"{ErrorPrefix} {Message}";
}

/// <summary>
/// Wrong type error.
/// </summary>
public class WrongTypeException : RedisException
{
    public WrongTypeException()
        : base("Operation against a key holding the wrong kind of value", "WRONGTYPE")
    {
    }
}

/// <summary>
/// Syntax error.
/// </summary>
public class SyntaxErrorException : RedisException
{
    public SyntaxErrorException(string message = "syntax error")
        : base(message)
    {
    }
}

/// <summary>
/// Invalid argument error.
/// </summary>
public class InvalidArgumentException : RedisException
{
    public InvalidArgumentException(string message)
        : base(message)
    {
    }
}

/// <summary>
/// Unknown command error.
/// </summary>
public class UnknownCommandException : RedisException
{
    public string CommandName { get; }

    public UnknownCommandException(string commandName)
        : base($"unknown command '{commandName}'")
    {
        CommandName = commandName;
    }
}

/// <summary>
/// Wrong number of arguments error.
/// </summary>
public class WrongArityException : RedisException
{
    public string CommandName { get; }

    public WrongArityException(string commandName)
        : base($"wrong number of arguments for '{commandName}' command")
    {
        CommandName = commandName;
    }
}

/// <summary>
/// Not an integer error.
/// </summary>
public class NotIntegerException : RedisException
{
    public NotIntegerException()
        : base("value is not an integer or out of range")
    {
    }
}

/// <summary>
/// Not a float error.
/// </summary>
public class NotFloatException : RedisException
{
    public NotFloatException()
        : base("value is not a valid float")
    {
    }
}
