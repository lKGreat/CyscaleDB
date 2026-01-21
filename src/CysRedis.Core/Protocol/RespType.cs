namespace CysRedis.Core.Protocol;

/// <summary>
/// RESP (Redis Serialization Protocol) data types.
/// </summary>
public enum RespType : byte
{
    /// <summary>
    /// Simple String: +OK\r\n
    /// </summary>
    SimpleString = (byte)'+',

    /// <summary>
    /// Error: -ERR message\r\n
    /// </summary>
    Error = (byte)'-',

    /// <summary>
    /// Integer: :1000\r\n
    /// </summary>
    Integer = (byte)':',

    /// <summary>
    /// Bulk String: $6\r\nfoobar\r\n
    /// </summary>
    BulkString = (byte)'$',

    /// <summary>
    /// Array: *2\r\n...
    /// </summary>
    Array = (byte)'*',

    // RESP3 types

    /// <summary>
    /// Null: _\r\n (RESP3)
    /// </summary>
    Null = (byte)'_',

    /// <summary>
    /// Boolean: #t\r\n or #f\r\n (RESP3)
    /// </summary>
    Boolean = (byte)'#',

    /// <summary>
    /// Double: ,1.23\r\n (RESP3)
    /// </summary>
    Double = (byte)',',

    /// <summary>
    /// Big Number: (3492890328409238509324850943850943825024385\r\n (RESP3)
    /// </summary>
    BigNumber = (byte)'(',

    /// <summary>
    /// Bulk Error: !21\r\nSYNTAX invalid syntax\r\n (RESP3)
    /// </summary>
    BulkError = (byte)'!',

    /// <summary>
    /// Verbatim String: =15\r\ntxt:Some string\r\n (RESP3)
    /// </summary>
    VerbatimString = (byte)'=',

    /// <summary>
    /// Map: %2\r\n... (RESP3)
    /// </summary>
    Map = (byte)'%',

    /// <summary>
    /// Set: ~3\r\n... (RESP3)
    /// </summary>
    Set = (byte)'~',

    /// <summary>
    /// Push: >3\r\n... (RESP3)
    /// </summary>
    Push = (byte)'>',
}
