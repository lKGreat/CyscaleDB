using System.Text;

namespace CysRedis.Core.Protocol;

/// <summary>
/// Represents a RESP protocol value.
/// </summary>
public readonly struct RespValue
{
    /// <summary>
    /// The type of this value.
    /// </summary>
    public RespType Type { get; }

    /// <summary>
    /// The raw value (for strings, errors, bulk strings).
    /// </summary>
    public ReadOnlyMemory<byte>? Bytes { get; }

    /// <summary>
    /// Integer value (for integers).
    /// </summary>
    public long? Integer { get; }

    /// <summary>
    /// Double value (for doubles).
    /// </summary>
    public double? Double { get; }

    /// <summary>
    /// Boolean value (for booleans).
    /// </summary>
    public bool? Boolean { get; }

    /// <summary>
    /// Array elements (for arrays, sets, maps).
    /// </summary>
    public RespValue[]? Elements { get; }

    /// <summary>
    /// Null value singleton.
    /// </summary>
    public static readonly RespValue Null = CreateNull();

    /// <summary>
    /// OK simple string singleton.
    /// </summary>
    public static readonly RespValue Ok = SimpleString("OK");

    /// <summary>
    /// PONG simple string singleton.
    /// </summary>
    public static readonly RespValue Pong = SimpleString("PONG");

    /// <summary>
    /// QUEUED simple string singleton.
    /// </summary>
    public static readonly RespValue Queued = SimpleString("QUEUED");

    /// <summary>
    /// Integer 0 singleton.
    /// </summary>
    public static readonly RespValue Zero = new(0);

    /// <summary>
    /// Integer 1 singleton.
    /// </summary>
    public static readonly RespValue One = new(1);

    /// <summary>
    /// Empty array singleton.
    /// </summary>
    public static readonly RespValue EmptyArray = Array();

    /// <summary>
    /// Empty bulk string singleton.
    /// </summary>
    public static readonly RespValue EmptyBulkString = BulkString(ReadOnlyMemory<byte>.Empty);

    private RespValue(RespType type, bool _)
    {
        Type = type;
        Bytes = null;
        Integer = null;
        Double = null;
        Boolean = null;
        Elements = null;
    }

    private static RespValue CreateNull() => new(RespType.Null, false);

    /// <summary>
    /// Creates an integer value.
    /// </summary>
    public RespValue(long value)
    {
        Type = RespType.Integer;
        Integer = value;
        Bytes = null;
        Double = null;
        Boolean = null;
        Elements = null;
    }

    /// <summary>
    /// Creates a double value.
    /// </summary>
    public RespValue(double value)
    {
        Type = RespType.Double;
        Double = value;
        Bytes = null;
        Integer = null;
        Boolean = null;
        Elements = null;
    }

    /// <summary>
    /// Creates a boolean value.
    /// </summary>
    public RespValue(bool value)
    {
        Type = RespType.Boolean;
        Boolean = value;
        Bytes = null;
        Integer = null;
        Double = null;
        Elements = null;
    }

    /// <summary>
    /// Creates a value with bytes.
    /// </summary>
    public RespValue(RespType type, ReadOnlyMemory<byte> bytes)
    {
        Type = type;
        Bytes = bytes;
        Integer = null;
        Double = null;
        Boolean = null;
        Elements = null;
    }

    /// <summary>
    /// Creates an array value.
    /// </summary>
    public RespValue(RespValue[] elements)
    {
        Type = RespType.Array;
        Elements = elements;
        Bytes = null;
        Integer = null;
        Double = null;
        Boolean = null;
    }

    /// <summary>
    /// Creates a simple string value.
    /// </summary>
    public static RespValue SimpleString(string value)
        => new(RespType.SimpleString, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// Creates an error value.
    /// </summary>
    public static RespValue Error(string message)
        => new(RespType.Error, Encoding.UTF8.GetBytes(message));

    /// <summary>
    /// Creates a bulk string value.
    /// </summary>
    public static RespValue BulkString(ReadOnlyMemory<byte> bytes)
        => new(RespType.BulkString, bytes);

    /// <summary>
    /// Creates a bulk string value from a string.
    /// </summary>
    public static RespValue BulkString(string value)
        => new(RespType.BulkString, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// Creates an array value.
    /// </summary>
    public static RespValue Array(params RespValue[] elements)
        => new(elements);

    /// <summary>
    /// Gets the string representation of this value.
    /// </summary>
    public string? GetString()
    {
        if (Bytes.HasValue)
            return Encoding.UTF8.GetString(Bytes.Value.Span);
        return null;
    }

    /// <summary>
    /// Gets whether this value is null.
    /// </summary>
    public bool IsNull => Type == RespType.Null || 
                          (Type == RespType.BulkString && !Bytes.HasValue) ||
                          (Type == RespType.Array && Elements == null);

    /// <summary>
    /// Gets the element count for arrays.
    /// </summary>
    public int Count => Elements?.Length ?? 0;

    /// <summary>
    /// Gets an element by index.
    /// </summary>
    public RespValue this[int index] => Elements?[index] ?? Null;

    public override string ToString()
    {
        return Type switch
        {
            RespType.SimpleString => $"+{GetString()}",
            RespType.Error => $"-{GetString()}",
            RespType.Integer => $":{Integer}",
            RespType.BulkString when IsNull => "$-1",
            RespType.BulkString => $"${Bytes?.Length}:{GetString()}",
            RespType.Array when IsNull => "*-1",
            RespType.Array => $"*{Elements?.Length}",
            RespType.Null => "_",
            RespType.Boolean => Boolean == true ? "#t" : "#f",
            RespType.Double => $",{Double}",
            _ => $"({Type})"
        };
    }
}
